(defpackage #:prevalence-multimaster/utils
  (:use #:cl)
  (:export
   #:path-to-string
   #:relative-path))
(in-package prevalence-multimaster/utils)


(defun path-to-string (path)
  (format nil "~A" path))


(defun relative-path (prefix-path full-path)
  "Given paths like this:

   (relative-path #P\"/Users/art/Dropbox/hacrm/\"
                  #P\"/Users/art/Dropbox/hacrm/bar/transaction-log-20190602T143842.xml\")

   returns:

   #P\"bar/transaction-log-20190602T143842.xml\""
  (make-pathname :directory (cons :relative
                                  (nthcdr (length (pathname-directory prefix-path))
                                          (pathname-directory full-path)))
                 :name (pathname-name full-path)
                 :type (pathname-type full-path)))


(defmethod s-serialization::serialize-xml-internal ((object pathname) stream serialization-state)
  (declare (ignore serialization-state))
  (write-string "<PATHNAME>" stream)
  (prin1 object stream)
  (write-string "</PATHNAME>" stream))


(defmethod s-serialization::deserialize-xml-finish-element-aux ((name (eql :pathname)) attributes parent-seed seed)
  (declare (ignorable attributes parent-seed))
  (read-from-string seed))
