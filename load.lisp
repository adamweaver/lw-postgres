(define-lw-system lw-pg ()
  (:system "LW-DATE")
  (:system "LW-JSON")
  (:system "LW-STRING")
  (:file "coerce" :depends-on "ffi")
  (:file "driver" :depends-on "coerce")
  (:file "ffi" :depends-on "package")
  (:file "orm" :depends-on "driver")
  (:file "package"))


