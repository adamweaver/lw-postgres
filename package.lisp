(defpackage :pg
  (:use :cl :ffi)
  (:export "ID" "QUERY" "QUERY-ONCE" "WITH-DATABASE" "DEFMODEL"))
