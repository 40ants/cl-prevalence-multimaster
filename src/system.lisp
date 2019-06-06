(defpackage #:prevalence-multimaster/system
  (:use #:cl)
  (:import-from #:cl-ppcre
                #:scan)
  (:import-from #:uiop
                #:merge-pathnames*
                #:truenamize
                #:ensure-directory-pathname)
  (:import-from #:log4cl)
  (:import-from #:cl-prevalence
                #:timetag
                #:copy-file
                #:get-directory
                #:guarded-prevalence-system)
  (:import-from #:cl-fad
                #:walk-directory
                #:list-directory)
  (:import-from #:prevalence-multimaster/utils
                #:path-to-string
                #:relative-path)
  (:export
   #:multimaster-system
   #:make-system
   #:with-system
   #:get-root-object
   #:reset
   #:mark-log-as-applied
   #:log-applied-p
   #:get-applied-logs
   #:get-name
   #:get-log-suffix
   #:is-transaction-log
   #:is-my-file
   #:get-root-path
   #:get-all-applied-logs))
(in-package prevalence-multimaster/system)


;; current system
(defvar *system*)


(defclass multimaster-system (guarded-prevalence-system)
  ((root-path :type pathname
              :documentation "A path where all syncronized masters store their database.
                              This could be, for example, \"~/Docker/my-app/\""
              :initarg :root-path
              :reader get-root-path)
   (name :type string
         :documentation "A system's name. It will be used to organize a folder inside of `root-path'."
         :initarg :name
         :reader get-name)))


(defun make-system (root-path name &key (class 'multimaster-system))
  (check-type root-path (or pathname
                            string))
  (check-type name string)
  
  (let* ((root-path (truenamize
                     (ensure-directory-pathname root-path)))
         (full-path (ensure-directory-pathname
                     (merge-pathnames* root-path
                                       name))))
    (make-instance class
                   :directory full-path
                   :root-path root-path
                   :name name)))


(defgeneric get-log-suffix (system)
  (:documentation "Returns a string to be used as a part of snapshot's or transaction log's filename.
                   Usually, this is a string with current time, but for unittests this could be
                   sequential id.")
  (:method ((system multimaster-system))
    (timetag)))


(defgeneric is-transaction-log (system path)
  (:documentation "Returns true, if path is a transaction log for this type of system.
                   For current transaction log (usually it is transaction.xml) it should
                   return nil.")
  (:method ((system multimaster-system) path)
    (declare (ignorable system))
    (let ((name (pathname-name path)))
      (scan "^transaction-log-\\d{8}T\\d{6}$"
            name))))


(defgeneric is-my-file (system path)
  (:documentation "Returns true, if path is under a system's database path.")
  (:method ((system multimaster-system) path)
    (let ((system-dir (truenamize (get-directory system))))
      (equal (pathname-directory system-dir)
             (pathname-directory path)))))


(defun mark-log-as-applied (system-name path &key (system *system*))
  (check-type system multimaster-system)
  (check-type path (or pathname string))
  (let* ((hash (cl-prevalence:get-root-object system :applied-logs))
         (root-path (get-root-path system))
         (path (path-to-string (relative-path root-path path))))
    (pushnew path (gethash system-name hash)
             :test 'equal)))


(defun get-all-applied-logs (&key (system *system*))
  (check-type system multimaster-system)
  (cl-prevalence:get-root-object system :applied-logs))


(defun get-applied-logs (&key (system *system*))
  (let ((hash (get-all-applied-logs system)))
    (gethash (get-name system) hash)))


(defun log-applied-p (path &key (system *system*))
  (check-type system multimaster-system)
  (check-type path pathname)
  (let ((hash (cl-prevalence:get-root-object system :applied-logs))
        (path (path-to-string (prevalence-multimaster/utils:relative-path
                               (get-root-path system)
                               path))))
    (member path
            (gethash (get-name system) hash)
            :test 'equal)))


(defun clone-other-master (system path-to-snapshot)
  (check-type system multimaster-system)
  (check-type path-to-snapshot pathname)
  (uiop:copy-file path-to-snapshot
                  (merge-pathnames* (get-directory system)
                                    "snapshot.xml"))
  (cl-prevalence:restore system)

  ;; After the data was restored, we need to pretend that the new system
  ;; has applied all logs, as the system used for replication.
  (let ((other-system-name (car (last (pathname-directory path-to-snapshot)))))
    (let ((hash (cl-prevalence:get-root-object system :applied-logs)))
      (setf (gethash (get-name system) hash)
            (copy-list (gethash other-system-name hash))))))


(defun search-other-system-snapshot (root-path)
  (check-type root-path pathname)
  (flet ((searcher (path)
           (when (string-equal (pathname-name path)
                               "snapshot")
             (return-from search-other-system-snapshot path))))
    (walk-directory root-path #'searcher)))


(defun directory-empty-p (path)
  (check-type path pathname)
  
  (and (uiop:directory-exists-p path)
       (null (list-directory path))))


(defmethod initialize-instance :after ((system multimaster-system) &rest args)
  (declare (ignorable args))

  (unless (cl-prevalence:get-root-object system :applied-logs)
    (log:info "Initializing applied logs list")
    (setf (cl-prevalence:get-root-object system :applied-logs)
          (make-hash-table :test 'equal)))
  
  (let ((dir (get-directory system)))
    (when (directory-empty-p dir)
      (let ((other-system-snapshot (search-other-system-snapshot (get-root-path system))))
        (when other-system-snapshot
          (log:info "Cloning other master")
          (clone-other-master system other-system-snapshot))))))


(defmacro with-system ((system) &body body)
  `(let ((*system* ,system))
     ,@body))


(defun get-root-object (name)
  (check-type name keyword)
  (cl-prevalence:get-root-object *system* name))


(defun (setf get-root-object) (value name)
  (setf (cl-prevalence:get-root-object *system* name)
        value))


(defun reset (&optional (system *system*))
  (unless (boundp '*system*)
           (error "Use with-system macro around the code, to bind current *system* variable or pass system as a parameter."))
  (setf (cl-prevalence::get-root-objects system)
        (make-hash-table)))
