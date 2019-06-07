(defpackage #:prevalence-multimaster/sync
  (:use #:cl)
  (:import-from #:cl-fad
                #:pathname-parent-directory)
  (:import-from #:cl-prevalence
                #:copy-file)
  (:import-from #:uiop
                #:truenamize)
  (:import-from #:cl-prevalence
                #:get-directory)
  (:import-from #:general-accumulator
                #:with-accumulator)
  (:import-from #:prevalence-multimaster/transaction
                #:define-transaction)
  (:import-from #:prevalence-multimaster/system
                #:get-num-transactions
                #:get-log-suffix
                #:is-my-file
                #:is-transaction-log
                #:log-applied-p
                #:with-system
                #:get-root-object
                #:*system*)
  (:import-from #:prevalence-multimaster/utils
                #:path-to-string)
  (:import-from #:cl-strings
                #:replace-all)
  (:export
   #:process
   #:sync-with-other-masters))
(in-package prevalence-multimaster/sync)


(defun discover-logs (system)
  "Searches files with transaction logs of other masters."
  (let* ((system-dir (truenamize (get-directory system)))
         (root-dir (pathname-parent-directory system-dir)))
    (with-accumulator (collect :list)
      (cl-fad:walk-directory root-dir
                             (lambda (path)
                               (when (and (is-transaction-log system path)
                                          (not (is-my-file system path)))
                                 (collect path))))
      (sort (collect)
            #'string<
            :key #'pathname-name))))


(define-transaction mark-log-as-applied (system-name path)
  (prevalence-multimaster/system:mark-log-as-applied system-name path))


(define-transaction apply-other-master-log (system-name path)
  ;; (unless (typep path 'string)
  ;;   (error "Please pass path as a simple-string, because cl-prevalence is unable to serialize pathnames."))

  (cond
    ;; This is transaction from another system
    ;; we just need to save this information into the applied-logs
    ((not (string-equal system-name
                        (prevalence-multimaster/system:get-name *system*)))
     (log:info "Applying log" system-name path)
     (mark-log-as-applied system-name path))
    (t ;; Otherwise we are really applying log to the system
     (log:info "Applying log" system-name path)
     (when (and path
                ;; Protect from applying log twice
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
             (mark-log-as-applied system-name path)))))))
  (values))


(defun is-processed-by-other-masters (system path)
  (let* ((relative (path-to-string
                    (prevalence-multimaster/utils:relative-path
                     (prevalence-multimaster/system:get-root-path system)
                     path)))
         (applied-logs (prevalence-multimaster/system:get-all-applied-logs :system system))
         (system-names (loop for system-name being the hash-keys in applied-logs
                             collect system-name))
         (path-applied-by (loop for system-name being the hash-keys in applied-logs
                                  using (hash-value filenames)
                                when (member relative filenames :test 'string=)
                                  collect system-name)))
    (= (length path-applied-by)
       (length system-names))))


(defun get-files-to-delete (system)
  (let ((system-directory (get-directory system)))
    (with-accumulator (collect :list)
      (cl-fad:walk-directory
       system-directory
       (lambda (path)
         ;; (when (cl-strings:ends-with (path-to-string path) "a/transaction-log-1.xml")
         ;;   (break))
         (when (and (is-transaction-log system path)
                    (is-processed-by-other-masters system path))
           (collect path)
           (collect (replace-all (path-to-string path)
                                 "transaction-log-"
                                 "snapshot-")))))
      (collect))))


(defun delete-old-logs (system)
  "Removes logs and snapshots which were processed by all masters.
   Only longs which belong to the given system are removed."
  (mapc 'uiop:delete-file-if-exists
        (get-files-to-delete system)))


(defun sync-with-other-masters (system &key (delete-old-logs t))
  (funcall (cl-prevalence:get-guard system)
           (lambda ()
             (cond
               ((> (get-num-transactions system)
                   0)
                (cl-prevalence:snapshot system)
                (setf (get-num-transactions system)
                      0))
               (t (log:info "Skipping snapshot because there wasn't any transactions between syncs.")))
     
             (with-system (system)
               (loop for path in (discover-logs system)
                     do (unless (log-applied-p path)
                          (apply-other-master-log
                           (prevalence-multimaster/system:get-name system)
                           path)))

       
               (when delete-old-logs
                 (delete-old-logs system))))))


(defun touch-file (path)
  "Creates an empty file."
  (uiop:with-output-file (stream path :if-does-not-exist :create)
    (declare (ignorable stream))))


(defmethod cl-prevalence:execute ((system prevalence-multimaster/system:multimaster-system) transaction)
  (prog1
   (call-next-method)
   
   (unless (member
            (cl-prevalence::get-function transaction)
            (list 'tx-mark-log-as-applied
                  'tx-apply-other-master-log))
     (funcall (cl-prevalence:get-guard system)
              #'(lambda ()
                  (incf (get-num-transactions system)))))))


(defmethod cl-prevalence:snapshot ((system prevalence-multimaster/system:multimaster-system))
  "Write to whole system to persistent storage resetting the transaction log"
  (let ((suffix (get-log-suffix system))
	(transaction-log (cl-prevalence::get-transaction-log system))
	(snapshot (cl-prevalence::get-snapshot system)))
    (cl-prevalence:close-open-streams system)
    
    (when (probe-file snapshot)
      (copy-file snapshot (merge-pathnames (make-pathname :name (cl-prevalence::get-snapshot-filename
                                                                 system
                                                                 suffix)
                                                          :type (cl-prevalence::get-file-extension system))
                                           snapshot)))
    
    (let ((new-log-filename (merge-pathnames
                             (make-pathname
                              :name (cl-prevalence::get-transaction-log-filename
                                     system
                                     suffix)
                              :type (cl-prevalence::get-file-extension system))
                             transaction-log)))
      (cond
        ((probe-file transaction-log)
         (copy-file transaction-log new-log-filename)
         (delete-file transaction-log))
        (t
         ;; If there is no real transaction log
         ;; we'll create an empty file to make
         ;; logs deletion work nicely.
         (touch-file new-log-filename)))
    
      (with-system (system)
        (mark-log-as-applied (prevalence-multimaster/system:get-name system)
                             new-log-filename)))
    
    (with-open-file (out snapshot
			 :direction :output :if-does-not-exist :create :if-exists :supersede)
      (funcall (cl-prevalence::get-serializer system) (cl-prevalence::get-root-objects system) out (cl-prevalence::get-serialization-state system)))))


