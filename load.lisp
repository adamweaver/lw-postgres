(define-lw-system lw-postgres ()
  (:system "LW-DATE")
  (:system "LW-JSON")
  (:system "LW-UTILS")
  (:file "coerce" :depends-on "ffi")
  (:file "driver" :depends-on "coerce")
  (:file "ffi" :depends-on "package")
  (:file "orm" :depends-on "driver")
  (:file "package"))


