(defpackage #:prevalence-multimaster/sync
  (:use #:cl)
  (:import-from #:cl-fad
                #:pathname-parent-directory)
  (:import-from #:uiop
                #:truenamize)
  (:import-from #:cl-prevalence
                #:get-directory)
  (:import-from #:general-accumulator
                #:with-accumulator)
  (:import-from #:cl-ppcre
                #:scan)
  (:import-from #:prevalence-multimaster/transaction
                #:define-transaction)
  (:import-from #:prevalence-multimaster/system
                #:mark-log-as-applied
                #:log-applied-p
                #:with-system
                #:get-root-object
                #:*system*)
  (:import-from #:prevalence-multimaster/utils
                #:path-to-string)
  (:export
   #:process
   #:sync-with-other-masters))
(in-package prevalence-multimaster/sync)


(defun discover-logs (system)
  "Searches files with transaction logs of other masters."
  (let* ((system-dir (truenamize (get-directory system)))
         (root-dir (pathname-parent-directory system-dir)))
    (with-accumulator (collect :list)
      (flet ((is-transaction-log (path)
               (let ((name (pathname-name path)))
                 (scan "^transaction-log-\\d{8}T\\d{6}$"
                       name)))
             (is-mine (path)
               (equal (pathname-directory system-dir)
                      (pathname-directory path))))
        
        (cl-fad:walk-directory root-dir
                               (lambda (path)
                                 (when (and (is-transaction-log path)
                                            (not (is-mine path)))
                                   (collect path))))
        (sort (collect)
              #'string<
              :key #'pathname-name)))))


(define-transaction apply-other-master-log (path)
  (unless (typep path 'string)
    (error "Please pass path as a simple-string, because cl-prevalence is unable to serialize pathnames."))
  
  (let ((path (probe-file path)))
    (when (and path
               (not (log-applied-p path)))
      (block apply-log
        (let ((position 0))
          (handler-bind ((s-xml:xml-parser-error 
                           #'(lambda (condition)
                               (format *standard-output* 
                                       ";; Warning: error during transaction log resystem: ~s:~A~%" 
                                       condition
                                       position)
                               (return-from apply-log))))
            (with-open-file (in path :direction :input)
              (loop
                (let ((transaction (funcall (cl-prevalence::get-deserializer *system*)
                                            in
                                            (cl-prevalence::get-serialization-state *system*))))
                  (setf position (file-position in))
                  (if transaction
                      (cl-prevalence::execute-on transaction *system*)
                      (return)))))
            (mark-log-as-applied path))))))
  (values))


(defun sync-with-other-masters (system &key b)
  (when b
    (break))
  (cl-prevalence:snapshot system)
  (when b
    (break))
  
  (with-system (system)
    (loop for path in (discover-logs system)
          for path-as-string = (path-to-string path)
          do (apply-other-master-log path-as-string))

    ;; осталось сделать вычистку транзакционных логов,
    ;; которые уже прочитаны всеми живыми репликами
    ))
