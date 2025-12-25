(in-package :pg)

(defun result-failure (pgresult)
  (not (find (result-status pgresult) `(,+command-ok+ ,+tuples-ok+))))

(defun get-results (pgresult types)
  (loop for y below (num-tuples pgresult)
        collect (loop for type in types
                      for x below (length types)
                      collect
                      (unless (plusp (get-is-null pgresult y x))
                        (let ((buffer (get-value pgresult y x))
                              (length (get-length pgresult y x)))
                          (case type
                            (:date (date:make-date (+ (floor (coerce-from-c-buffer pgresult buffer length :integer :network) 1000000) +epoch+)))
                            (:json (json:decode (nsubseq (coerce-from-c-buffer pgresult buffer length :json :network) 1)))
                            (:boolean (plusp (coerce-from-c-buffer pgresult buffer 1 :boolean)))
                            (:fixed (/ (coerce-from-c-buffer pgresult buffer length type :network x) 100000))
                            (t (coerce-from-c-buffer pgresult buffer length type :network x))))))))

(defun coerce-to-c-buffer (value type)
  (case type
    (:date (coerce-to-new-c-buffer (* (- (date:seconds value) +epoch+) 1000000) :type :long :format :network))
    (:boolean (coerce-to-new-c-buffer (if (or (null value) (and (numberp value) (zerop value))) #\Null #\t)))
    (:array
     ;; PG array format is a little unusual.
     ;; From https://stackoverflow.com/questions/4016412/postgresqls-libpq-encoding-for-binary-transport-of-array-data
     ;; For example, an array of 4 integers would look like:
     ;; 0x00000001 0x00000000 0x00000017 0x00000004 0x00000001 0x00000004 0x00000004 0x00000004 0x00000004
     ;; The first integer is 1 dimension, the second integer is no NULL bitmap (so none of the array's values are NULL),
     ;; the third integer is the OID of the elements (23, INT4OID), the fourth integer is how big the first dimension is (4),
     ;; the fifth integer is the starting index of the first dimension. After that is raw array data, in sequential order,
     ;; each element prefixed by its length (4 bytes for each integer).
     (let ((vector (apply #'vector (nconc (list 1 0 +int4-oid+ (length value) 1) (loop for v in value collect 4 collect v)))))
       (coerce-to-new-c-buffer vector :type :integer-array :format :network)))
    (:json (coerce-to-new-c-buffer (if (stringp value) value (json:encode value)) :format :network))
    (:string (coerce-to-new-c-buffer (if (stringp value) value (princ-to-string value)) :format :network))
    (:fixed (coerce-to-new-c-buffer (floor (* value 100000)) :format :network))
    (t (coerce-to-new-c-buffer value :format :network))))

(defun coerce-from-c-buffer (result buffer length type &optional (order :host) (col 0))
  "Return a value of TYPE from BUFFER"
  (case type
    ((:string :json)
     (convert-from-foreign-string buffer :external-format :utf-8 :length length))

    ((:integer :boolean :fixed)
     (coerce-integer-from-c-buffer result buffer length order col))

    (:real
     #+little-endian (when (eq order :network) (reverse-c-byte-order buffer length))
     (with-coerced-pointer (ptr :type (if (= length (size-of :float)) :float :double)) buffer
       (dereference ptr)))
    (t
     (let ((array (make-array length :element-type '(unsigned-byte 8))))
       (with-coerced-pointer (ptr :type '(:unsigned :char)) buffer
         (loop for i below length do (setf (aref array i) (dereference ptr :index i)))
         array)))))

(defun coerce-integer-from-c-buffer (result buffer length order col)
  (let ((type (field-type result col)))
    #+little-endian
    (when (and (eq order :network) (> length 1) (find type +integer-oids+ :test #'=))
      (reverse-c-byte-order buffer length))
    (cond ((= type +numeric-oid+) (coerce-numeric-from-c-buffer buffer length))
          ((= type +text-oid+) (parse-integer (convert-from-foreign-string buffer :external-format :utf-8 :length length)))
          ((null (find type +integer-oids+ :test #'=)) (error "Unimplemented int conversion from OID type ~A" type))
          ((= length 1) (with-coerced-pointer (ptr :type '(:unsigned :char)) buffer (dereference ptr)))
          ((= length 2) (with-coerced-pointer (ptr :type :short) buffer (dereference ptr)))
          ((= length 4) (with-coerced-pointer (ptr :type :int) buffer (dereference ptr)))
          ((= length 8) (with-coerced-pointer (ptr :type :long) buffer (dereference ptr)))
          ((= length 16) (with-coerced-pointer (ptr :type '(:long :long)) buffer (dereference ptr)))
          (t (error "Unknown integer length ~A of type ~A in column ~A" length type col)))))

(defun coerce-numeric-from-c-buffer (buffer length)
  (declare (ignore buffer))
  (error "Unimplemented numeric conversion ~A" length))

(defun coerce-to-new-c-buffer (value &key type (format :host))
  "Return (values c-format-buffer (length c-format-buffer))"
  (typecase value
    (string
     (multiple-value-bind (buffer length bytes) (convert-to-foreign-string value :external-format :utf-8 :null-terminated-p nil)
       (declare (ignore length))
       (values buffer bytes)))
    (array
     (cond ((eq type :integer-array)
            ;; postgresql 32 bit weird format
            (let* ((length (length value))
                   (buffer (allocate-foreign-object :type '(:unsigned :char) :nelems (* length 4))))
              (with-coerced-pointer (ptr :type :int) buffer
                (loop for i below length for v across value do
                  (setf (dereference ptr :index i) v)
                  (when (eq format :network) (reverse-c-byte-order buffer 4 (* i 4)))))
              (values buffer (* length 4))))

           ((eq type :string-array)
            (error "string array pg format not implemented"))

           ;; standard 8 bit format
           (t (let* ((length (length value))
                     (buffer (allocate-foreign-object :type '(:unsigned :char) :nelems length)))
                (with-coerced-pointer (ptr :type '(:unsigned :byte)) buffer
                  (loop for i from 0 for b across value do (setf (dereference ptr :index i) b)))
                (values buffer length)))))

    (character
     (values (allocate-foreign-object :type '(:unsigned :char) :fill (char-int value)) 1))
    (number
     (let* ((type (or type (if (integerp value) :int :float)))
            (buffer (allocate-foreign-object :type '(:unsigned :char) :nelems (size-of type))))
       (with-coerced-pointer (ptr :type type) buffer (setf (dereference ptr) value))
       #+little-endian (when (eq format :network) (reverse-c-byte-order buffer (size-of type)))
       (values buffer (size-of type))))
    (null
     (values (copy-pointer *null-pointer* :type '(:unsigned :char)) 0))
    (t
     (cond
       ;; hash table or list to be treated as alist
       ((or (hash-table-p value) (and (consp value) (consp (car value))))
        (coerce-to-new-c-buffer (json:encode value) :type type :format format))
       ;; list to be treated as array
       ((consp value)
        (coerce-to-new-c-buffer (apply #'vector value) :type :array :format format))
       (t
        (error "Unhandled conversion to C from lisp value ~S" value))))))

(defun coerce-to-old-c-buffer (value length buffer &key type (format :host))
  "Stuff VALUE into BUFFER of max LENGTH. Return number of bytes used"
  (typecase value
    (string
     (with-foreign-string (copy length bytes :external-format :utf-8 :null-terminated-p nil) value
       (with-coerced-pointer (ptr :type '(:signed :char)) buffer
         (replace-foreign-array ptr copy :end1 length :allow-sign-mismatch t))
       bytes))
    (number
     (let* ((type (or type (if (integerp value) :int :float))))
       (with-coerced-pointer (ptr :type type) buffer (setf (dereference ptr) value))
       #+little-endian (when (eq format :network) (reverse-c-byte-order buffer (size-of type)))
       (size-of type)))
    (array
     (let ((array-len (min length(length value))))
       (with-coerced-pointer (ptr :type '(:unsigned :byte)) buffer
         (loop for i below array-len for b across value do (setf (dereference ptr :index i) b)))
       array-len))
    (null
     0)
    (t
     (if (find (type-of value) '(hash-table cons) :test #'eq)
         (coerce-to-old-c-buffer (json:encode value) length :type type :format format)
         (error "Unhandled conversion to C from lisp value ~S" value)))))

