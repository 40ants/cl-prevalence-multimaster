(defpackage #:prevalence-multimaster-test/utils
  (:use #:cl)
  (:import-from #:prevalence-multimaster/transaction
                #:define-transaction)
  (:import-from #:prevalence-multimaster/utils
                #:path-to-string)
  (:import-from #:hamcrest/matchers
                #:assertion-error)
  (:export
   #:add-line
   #:get-lines
   #:matches))
(in-package prevalence-multimaster-test/utils)


(define-transaction %add-line (text)
  (push text
        (prevalence-multimaster/system:get-root-object :lines)))

(defun add-line (system text)
  "We need this helper to simplify switching between stores."
  (prevalence-multimaster/system:with-system (system)
    (%add-line text)))


(defun get-lines (store)
  (cl-prevalence:get-root-object store :lines))


(defun matches (regex)
  (lambda (obj)
    (let ((as-string (etypecase obj
                       (pathname (path-to-string obj))
                       (string obj)))
          (regex (format nil "^~A$" regex)))
      (unless (cl-ppcre:scan regex as-string)
        (error 'assertion-error
               :reason (format nil "Object ~S does not match to ~A regex."
                               obj
                               regex))))))
