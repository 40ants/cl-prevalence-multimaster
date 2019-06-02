(defpackage #:prevalence-multimaster/system
  (:use #:cl)
  (:import-from #:uiop
                #:merge-pathnames*
                #:truenamize
                #:ensure-directory-pathname)
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
   #:get-applied-logs))
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


(defun make-system (root-path name)
  (check-type root-path (or pathname
                            string))
  (check-type name string)
  
  (let* ((root-path (truenamize
                     (ensure-directory-pathname root-path)))
         (full-path (ensure-directory-pathname
                     (merge-pathnames* root-path
                                       name))))
    (make-instance 'multimaster-system
                   :directory full-path
                   :root-path root-path
                   :name name)))


(defun mark-log-as-applied (path &key (system *system*))
  (check-type system multimaster-system)
  (check-type path pathname)
  (let* ((hash (cl-prevalence:get-root-object system :applied-logs))
         (root-path (get-root-path system))
         (path (path-to-string (relative-path root-path path))))
    (pushnew path (gethash (get-name system) hash)
             :test 'equal)))


(defun get-applied-logs (&key (system *system*))
  (check-type system multimaster-system)
  (let ((hash (cl-prevalence:get-root-object system :applied-logs)))
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


;; TODO: REMOVE THIS METHOD
(defmethod cl-prevalence::get-transaction-log-filename :around ((system multimaster-system) &optional suffix)
  "Keep track transaction log versions produced by the system, to prevent repetitive loading of them by other masters during their initializations."
  (call-next-method))


(defmethod cl-prevalence:snapshot ((system multimaster-system))
  "Write to whole system to persistent storage resetting the transaction log"
  (let ((timetag (timetag))
	(transaction-log (cl-prevalence::get-transaction-log system))
	(snapshot (cl-prevalence::get-snapshot system)))
    (cl-prevalence:close-open-streams system)
    
    (when (probe-file snapshot)
      (copy-file snapshot (merge-pathnames (make-pathname :name (cl-prevalence::get-snapshot-filename system timetag)
                                                          :type (cl-prevalence::get-file-extension system))
                                           snapshot)))
    
    (when (probe-file transaction-log)
      (let ((new-log-filename (merge-pathnames (make-pathname :name (cl-prevalence::get-transaction-log-filename system timetag)
                                                              :type (cl-prevalence::get-file-extension system))
                                               transaction-log)))
        (when timetag
          (mark-log-as-applied new-log-filename :system system))
      
        (copy-file transaction-log new-log-filename))
      (delete-file transaction-log))
    
    (with-open-file (out snapshot
			 :direction :output :if-does-not-exist :create :if-exists :supersede)
      (funcall (cl-prevalence::get-serializer system) (cl-prevalence::get-root-objects system) out (cl-prevalence::get-serialization-state system)))))


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
  
  (setf (cl-prevalence:get-root-object system :applied-logs)
        (make-hash-table :test 'equal))
  
  (let ((dir (get-directory system)))
    (when (directory-empty-p dir)
      (let ((other-system-snapshot (search-other-system-snapshot (get-root-path system))))
        (when other-system-snapshot
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
