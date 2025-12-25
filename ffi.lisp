(in-package :pg)

(define-c-typedef pgconn  (:pointer :void))
(define-c-typedef pgresult (:pointer :void))
(define-c-typedef oid (:unsigned :int))

(eval-when (:compile-toplevel :load-toplevel :execute)
  (defconstant +bool-oid+ 16)
  (defconstant +bytea-oid+ 17)
  (defconstant +int8-oid+ 20)
  (defconstant +int2-oid+ 21)
  (defconstant +int4-oid+ 23)
  (defconstant +int4-array-oid+ 1007)
  (defconstant +text-array-oid+ 1009)
  (defconstant +text-oid+ 25)
  (defconstant +float8-oid+ 701)
  (defconstant +timestamp-oid+ 1114)
  (defconstant +json-oid+ 114)
  (defconstant +numeric-oid+ 1700)
  (defconstant +status-ok+ 0)
  (defconstant +status-bad+ 1)
  (defconstant +command-ok+ 1)
  (defconstant +tuples-ok+ 2)
  (defconstant +epoch+ 3155644800)
  (defconstant +integer-oids+ #(20 21 23 1114 1184)))


(defun lisp-type->postgresql-oid (type)
  (case type
    (:integer +int4-oid+)
    (:fixed +int4-oid+)
    (:boolean +bool-oid+)
    (:blob +bytea-oid+)
    (:real +float8-oid+)
    (:date +timestamp-oid+)
    (:json +json-oid+)
    (:array +int4-array-oid+)
    (:array-string +text-array-oid+)
    (t +text-oid+)))

(define-foreign-function (status "PQstatus") ((conn pgconn))
  :result-type :int)

(define-foreign-function (set-db-login "PQsetdbLogin")
    ((host (:reference-pass :ef-mb-string :allow-null t))
     (port (:reference-pass :ef-mb-string :allow-null t))
     (opts (:reference-pass :ef-mb-string :allow-null t))
     (tty (:reference-pass :ef-mb-string :allow-null t))
     (dbname (:reference-pass :ef-mb-string :allow-null t))
     (user (:reference-pass :ef-mb-string :allow-null t))
     (pwd (:reference-pass :ef-mb-string :allow-null t)))
  :result-type pgconn :module 'postgresql)

(define-foreign-function (finish "PQfinish") ((conn pgconn))
  :result-type :void :module 'postgresql)

(define-foreign-function (error-message "PQerrorMessage") ((conn pgconn))
  :result-type (:reference-return :ef-mb-string) :module 'postgresql)

(define-foreign-function (exec "PQexec") ((conn pgconn) (command (:reference-pass :ef-mb-string)))
  :result-type pgresult :module 'postgresql)

(define-foreign-function (prepare "PQprepare")
    ((conn pgconn) (statement-name (:reference-pass :ef-mb-string)) (query (:reference-pass :ef-mb-string))
                   (num-params :int) (param-types (:pointer oid)))
  :result-type pgresult :module 'postgresql)

(define-foreign-function (exec-prepared "PQexecPrepared")
    ((conn pgconn) (name (:reference-pass :ef-mb-string)) (num-params :int) (param-values (:pointer (:pointer (:unsigned :char))))
                   (param-lengths (:pointer :int)) (param-formats (:pointer :int)) (result-format :int))
  :result-type pgresult :module 'postgresql)

(define-foreign-function (result-status "PQresultStatus") ((result pgresult))
  :result-type :int :module 'postgresql)

(define-foreign-function (clear "PQclear") ((result pgresult))
  :result-type :void :module 'postgresql)

(define-foreign-function (num-tuples "PQntuples") ((result pgresult))
  :result-type :int :module 'postgresql)

(define-foreign-function (field-format "PQfformat") ((result pgresult) (col :int))
  :result-type :int :module 'postgresql)

(define-foreign-function (field-type "PQftype") ((result pgresult) (col :int))
  :result-type :int :module 'postgresql)

(define-foreign-function (get-value "PQgetvalue") ((result pgresult) (row :int) (col :int))
  :result-type (:pointer :char) :module 'postgresql)

(define-foreign-function (get-is-null "PQgetisnull") ((result pgresult) (row :int) (col :int))
  :result-type :int :module 'postgresql)

(define-foreign-function (get-length "PQgetlength") ((result pgresult) (row :int) (col :int))
  :result-type :int :module 'postgresql)

