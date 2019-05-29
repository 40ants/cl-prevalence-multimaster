(defpackage #:prevalence-multimaster/utils
  (:use #:cl)
  (:export
   #:path-to-string
   #:relative-path))
(in-package prevalence-multimaster/utils)


(defun path-to-string (path)
  (format nil "~A" path))


(defun relative-path (prefix-path full-path)
  (make-pathname :directory (cons :relative
                                  (nthcdr (length (pathname-directory prefix-path))
                                          (pathname-directory full-path)))
                 :name (pathname-name full-path)
                 :type (pathname-type full-path)))
