(in-package :pg)

(defvar *db* nil
  "Default database connection")

(defstruct database
  handle cache)

(defstruct statement
  database name sql params result-types)

(defun open-connection (host port database username password)
  (let ((conn (set-db-login host port nil nil database username password)))
    (cond ((null-pointer-p conn) (error "PostgreSQL connection failure"))
          ((/= (status conn) +status-ok+) (error (error-message conn)))
          (t (exec conn "DEALLOCATE ALL") (make-database :handle conn :cache (make-hash-table :test #'equalp))))))

(defun close-connection (database)
  (loop for value being the hash-values of (database-cache database) do (unprepare value))
  (finish (database-handle database)))

(defun try-command (database command)
  (let ((result (exec (database-handle database) command)))
    (if (or (null-pointer-p result) (result-failure result))
        (error (error-message (database-handle database)))
        t)))

(defun begin-transaction (database)
  (try-command database "START TRANSACTION ISOLATION LEVEL REPEATABLE READ"))

(defun commit-transaction (database)
  (try-command database "COMMIT"))

(defun rollback-transaction (database)
  (try-command database "ROLLBACK"))

(defun prepare-sql-and-oids (query types)
  (let ((sql "") (oids nil) (type-list nil) (index 0))
    (loop for a in (flatten (mklist query))
          if (keywordp a)
            do (let ((type (assoc a types)))
                 (setf sql (concatenate 'string sql (format nil "$~D" (incf index)))
                       oids (cons (lisp-type->postgresql-oid (cdr type)) oids)
                       type-list (cons type type-list)))
          else
            do (setf sql (concatenate 'string sql a))
          finally (return (list sql index (nreverse oids) (nreverse type-list))))))

(defun prepare (database name query types result-types)
  (destructuring-bind (sql nelems oid-list type-list) (prepare-sql-and-oids (flatten (mklist query)) types)
    (with-dynamic-foreign-objects ((oids oid :nelems nelems :initial-contents oid-list))
      (let ((result (prepare (database-handle database) (or name "") sql nelems oids)))
        (if (or (null-pointer-p result) (result-failure result))
            (error (error-message (database-handle database)))
            (make-statement :database database :name name :params type-list :result-types result-types :sql sql))))))

(defun unprepare (statement)
  (when (and (statement-name statement) (string/= (statement-name statement) ""))
    (exec (database-handle (statement-database statement)) (format nil "DEALLOCATE ~A" (statement-name statement))))
  t)

(defun query-statement (statement params)
  (let ((handle (database-handle (statement-database statement))) bufs lengths (null-pointer (make-pointer :address 0 :type '(:unsigned :char))))
    (unwind-protect
         (progn
           (loop for (keyword . type) in (statement-params statement)
                 for value = (getf params keyword)
                 if (or value (eq type :boolean))
                   do (multiple-value-bind (buffer length) (coerce-to-c-buffer value type)
                        (setf bufs (cons buffer bufs) lengths (cons length lengths)))
                 else
                   do (setf bufs (cons null-pointer bufs) lengths (cons 0 lengths)))

           (with-dynamic-foreign-objects
               ((values (:pointer (:unsigned :char)) :nelems (length bufs) :initial-contents (reverse bufs))
                (vlens :int :nelems (length lengths) :initial-contents (reverse lengths))
                (format :int :nelems (length lengths) :initial-element 1))
             (let ((results (exec-prepared handle (or (statement-name statement) "") (length bufs) values vlens format 1)))
               (if (or (null-pointer-p results) (result-failure results))
                   (error (error-message handle))
                   (unwind-protect (get-results results (statement-result-types statement))
                     (clear results))))))
      (map nil #'free-foreign-object bufs))))

(defun memoise-prepared-statement (database sql name types result-types)
  (or (gethash name (database-cache database))
      (setf (gethash name (database-cache database)) (prepare database name sql types result-types))))

(defun query (sql &key name types result-types params flatten vector)
  (flet ((process (results)
           (cond ((and flatten vector) (coerce (flatten results) 'vector))
                 (flatten (flatten results))
                 (vector (map 'vector (lambda (row) (coerce row 'vector)) results))
                 (t results))))
    (process (if name
                 (query-statement (memoise-prepared-statement *db* sql name types result-types) params)
                 (query-once *db* sql types result-types params)))))

(defun query-once (database sql types result-types params)
  (let ((ps (prepare database "" sql types result-types)))
    (prog1 (query-statement ps params)
      (unprepare ps))))

(defgeneric id (object)
  (:documentation "Get the identifier for OBJECT - e.g. database table rowid")
  (:method ((object t))
    object))
