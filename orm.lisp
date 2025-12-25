(in-package :pg)

(defmacro with-database ((host port database username password) &body body)
  `(lw:if-let (*db* (open-connection ,host ,port ,database ,username ,password))
              (unwind-protect (handler-bind ((error (lambda (e) (clog (princ-to-string e)) (rollback-transaction *db*))))
                                (begin-transaction *db*)
                                (multiple-value-prog1 (progn ,@body)
                                  (commit-transaction *db*)))
                (close-connection *db*))
              (error "Unable to open database ~S" ,database)))

(defmacro defmodel (table slots &key join search fts user inflate order)
  (labels ((make-slot-from-spec (spec)
             `(,(car spec) :initarg ,(intern (symbol-name (car spec)) "KEYWORD") :accessor ,(car spec) :initform nil))

           (option-p (arg options)
             (find arg options :test #'eq))

           (sql (sym)
             (when sym
               (let ((str (symbol-name sym)) (reserved '("DEFAULT" "END" "USER" "PRIMARY" "TRANSACTION" "FOREIGN" "DATE")))
                 (if (find str reserved :test #'string-equal)
                     (format nil "~(\"~A\"~)" str)
                     (string-downcase str)))))

           (search-p (spec)
             (or (option-p :search (cddr spec)) (unique-p spec)))

           (unique-p (spec)
             (option-p :unique (cddr spec)))

           (search-fn ()
             (intern (format nil "FIND-~:@(~A~)" (english:pluralise-word (symbol-name table)))))

           (keyword (sym)
             (intern (symbol-name sym) "KEYWORD"))

           (parameter (sym)
             (if (string-equal sym "LIKE")
                 `(when ,sym (format nil "%~A%" (substitute #\% #\Space ,sym :test #'char=)))
                 sym))

           (type-from-list (list)
             (if (keywordp (cadr list))
                 (cadr list)
                 :integer))

           (initform-from-list (list &key parameter)
             (loop for (sym) in list collect (keyword sym) collect (if parameter (parameter sym) sym)))

           (cons-type-from-list (list)
             (cons (keyword (car list)) (type-from-list list)))

           (make-1=1 (spec)
             (destructuring-bind (sym type &rest ignore) spec
               (declare (ignore ignore))
               (list sym type (format nil "~@[~A.~]~A = " (when join (sql table)) (sql sym)) (keyword sym))))

           (make-search-by-unique (unique)
             (destructuring-bind (sym &rest ignore) unique
               (declare (ignore ignore))
               (let ((fn (intern (format nil "FIND-~A-BY-~A" table sym))))
                 `(defun ,fn (,sym ,@(when user '(&key (user (id config:*user*)))))
                    (car (,(search-fn) ,(keyword sym) ,sym ,@(when user '(:user user)) :limit 1))))))

           (need-user-p (args)
             (and user (not (find "user" (mapcar (compose #'symbol-name #'car) args) :test #'string-equal))))

           (remove-user (args &optional (key #'identity))
             (remove "user" args :key (compose #'symbol-name key) :test #'string-equal))

           (make-search (args)
             `(defun ,(search-fn) (&key ,@(remove-user (mapcar #'car args)) ,@(when user '((user (id config:*user*)))) ,@(when fts '(fts)) limit)
                (loop for ,(mapcan #'inflated-places slots) in ,(make-query (if (need-user-p args) (cons '(user :integer "u.\"user\" = " :user) args) args))
                      collect (make-instance ',table ,@(mapcan #'make-initialiser slots)))))

           (make-query (args)
             (let* ((args-with-user (if user (cons '(user :integer "u.\"user\" = " :user) (remove-user args #'car)) args))
                    (args-with-fts (if fts (cons `(fts :string ,@(make-fts-search-sql fts)) args-with-user) args-with-user)))
               `(query ,(make-sql slots args-with-fts)
                       :types ',(mapcar #'cons-type-from-list args-with-fts)
                       :result-types ',(mapcan #'inflated-type-from-list slots)
                       :params (list ,@(initform-from-list args-with-fts :parameter t)))))

           (make-fts-search-sql (fts)
             (list (format nil "TO_TSVECTOR('english', ~{COALESCE(~A, '')~^ || ' ' || ~}) @@ TO_TSQUERY('english', " (mapcar #'sql fts)) :fts ")"))

           (make-sql (slots args)
             `(let (wheres)
                ,@(loop for spec in args collect `(when ,(car spec) (setf wheres (cons ',(cddr spec) wheres))))
                (flatten (list ,(format nil "SELECT ~{~A~^, ~} FROM ~A~{ LEFT JOIN ~A~}" (mapcan #'qualified-slot-names slots) (sql table) join)
                               (when wheres (cons " WHERE " (flatten (intersperse " AND " wheres))))
                               ,(when order (format nil " ORDER BY ~A" order))
                               (when limit (format nil " LIMIT ~D" limit))))))

           (inflated-type-from-list (slot)
             (destructuring-bind (sym type &rest rest) slot
               (declare (ignore rest))
               (lw:if-let (inflate (find sym inflate :test #'eq :key #'car))
                          (mapcan #'inflated-type-from-list (cdr inflate))
                          (cons (if (keywordp type) type :integer) nil))))

           (inflated-places (slot &optional parents)
             (destructuring-bind (sym &rest rest) slot
               (declare (ignore rest))
               (lw:if-let (inflate (find sym inflate :test #'eq :key #'car))
                          (loop for subslot in (cdr inflate) nconc (inflated-places subslot (cons sym parents)))
                          (list (intern (format nil "~{~A-~}~A" (reverse parents) sym))))))

           (qualified-slot-names (slot &optional parent)
             (destructuring-bind (sym &rest rest) slot
               (declare (ignore rest))
               (lw:if-let (inflate (find sym inflate :test #'eq :key #'car))
                          (loop for subslot in (cdr inflate) nconc (qualified-slot-names subslot sym))
                          (list (format nil "~@[~A.~]~A" (sql (or parent (and join table))) (sql sym))))))

           (make-initialiser (slot &optional parents)
             (destructuring-bind (sym type &rest rest) slot
               (declare (ignore rest))
               (lw:if-let (inflate (find sym inflate :test #'eq :key #'car))
                          (list (keyword sym) `(when ,(intern (format nil "~{~A-~}~A-ID" (reverse parents) sym))
                                                 (make-instance ',(if (keywordp type) sym type)
                                                                ,@(loop for subslot in (cdr inflate) nconc (make-initialiser subslot (cons sym parents))))))
                          (list (keyword sym) (if parents (intern (format nil "~{~A-~}~A" (reverse parents) sym)) sym)))))

;;; ============================================================================
;;; UPDATER, INSERTER, DELETER
;;; ============================================================================

           (supplied-p (sym)
             (intern (format nil "~A-SUPPLIED-P" sym)))

           (updater-cons (spec)
             `(,(car spec) nil ,(supplied-p (car spec))))

           (make-updater ()
             `(defun ,(intern (format nil "UPDATE-~A" table)) (,table &key ,@(mapcar #'updater-cons (cdr slots)))
                (let (updated)
                  ,@(loop for (sym) in (cdr slots)
                          collect `(when ,(supplied-p sym)
                                     (setf (,sym ,table) ,sym updated (cons '(,(format nil "~A = " (sql sym)) ,(keyword sym)) updated))))
                  (when updated
                    (query (flatten (list ,(format nil "UPDATE ~A SET " (sql table)) (intersperse ", " updated) " WHERE id = " :id))
                           :types ',(mapcar #'cons-type-from-list slots)
                           :params (list :id (id ,table) ,@(initform-from-list (cdr slots)))))
                  ,table)))

           (make-inserter ()
             (let ((slots (cdr slots)) (syms (mapcar #'car (cdr slots))))
               `(defun ,(intern (format nil "CREATE-~A" table)) (&key ,@syms)
                  (let ((id (query '(,(format nil "INSERT INTO ~A (~{~A~^, ~}) VALUES (" (sql table) (mapcar #'sql syms))
                                     ,@(intersperse ", " (mapcar #'keyword syms)) ") RETURNING id")
                                   :name ,(format nil "INSERT-~A" table)
                                   :result-types '(:integer)
                                   :types ',(mapcar #'cons-type-from-list slots)
                                   :flatten t
                                   :params (list ,@(initform-from-list slots)))))
                    (make-instance ',table :id (car id) ,@(initform-from-list slots))))))

           (make-trash ()
             `(defun ,(intern (format nil "DELETE-~A" table)) (id)
                (query '(,(format nil "DELETE FROM ~A WHERE id = " (sql table)) :id)
                       :name ,(format nil "DELETE-~A" table)
                       :types '((:id . :integer))
                       :params (list :id id)))))

    `(progn
       (defclass ,table () ,(mapcar #'make-slot-from-spec slots))
       ,(make-search (append (mapcar #'make-1=1 (remove-if-not #'search-p slots)) search))
       ,@(mapcar #'make-search-by-unique (remove-if-not #'unique-p slots))
       ,(make-updater)
       ,(make-inserter)
       ,(make-trash))))
