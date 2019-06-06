(defpackage #:prevalence-multimaster-test/utils
  (:use #:cl)
  (:import-from #:prevalence-multimaster/system)
  (:import-from #:prevalence-multimaster/transaction
                #:define-transaction)
  (:import-from #:prevalence-multimaster/utils
                #:relative-path
                #:path-to-string)
  (:import-from #:hamcrest/matchers
                #:assertion-error)
  (:import-from #:uiop
                #:truenamize)
  (:import-from #:general-accumulator
                #:with-accumulator)
  (:export
   #:add-line
   #:get-lines
   #:matches
   #:get-dir-tree
   #:dump-state))
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

(defun get-dir-tree (root)
  (let ((root (truenamize root)))
    (with-accumulator (collect :list)
      (cl-fad:walk-directory root
                             (lambda (path)
                               (collect (relative-path root path))))
      (collect))))


(defun dump-state (title &rest stores)
  "Dumps state of applied logs from stores and contents of the test-m2m directory."
  (format t "~2&=== ~A~%" title)
  (loop for store in stores
        for name = (prevalence-multimaster/system::get-name store)
        for applied-logs = (cl-prevalence:get-root-object store :applied-logs)
        do (format t "~2&~A:~%" name)
           (loop for system-name being the hash-keys in applied-logs
                 using (hash-value logs)
                 do (format t "  ~A:~%" system-name)
                    (loop for filename in logs
                          do (format t "    ~A~%" filename))))
  (format t "~2&Filesystem:~%")
  (loop for filename in (get-dir-tree "test-m2m")
        do (format t "  ~A~%" filename)))
