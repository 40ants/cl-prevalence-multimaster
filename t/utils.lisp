(defpackage #:prevalence-multimaster-test/utils
  (:use #:cl)
  (:import-from #:prevalence-multimaster/system
                #:make-system
                #:multimaster-system)
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
  (:import-from #:cl-ppcre
                #:scan)
  (:export
   #:add-line
   #:get-lines
   #:matches
   #:get-dir-tree
   #:dump-state
   #:make-store
   #:test-multimaster
   #:get-transaction-id
   #:get-transactions
   #:add-timestamp
   #:get-timestamps))
(in-package prevalence-multimaster-test/utils)


(defclass test-multimaster (multimaster-system)
  ((transaction-id :initform 0
                   :accessor get-transaction-id)
   (transactions :initform nil
                 :documentation "Keeps all transactions since last snapshot."
                 :accessor get-transactions)))


(defmethod cl-prevalence:execute :after ((system test-multimaster) transaction)
  (push transaction
        (get-transactions system)))


(defmethod cl-prevalence:snapshot :after ((system test-multimaster))
  (setf (get-transactions system)
        nil))


(defmethod prevalence-multimaster/system:get-log-suffix ((system test-multimaster))
  (let ((value (incf (get-transaction-id system))))
    (format nil "~A" value)))


(defmethod prevalence-multimaster/system:is-transaction-log ((system test-multimaster) path)
  (declare (ignorable system))
  (let ((name (pathname-name path)))
    (scan "^transaction-log-\\d+$"
          name)))


(defun make-store (name)
  (make-system "test-m2m" name :class 'test-multimaster))



(define-transaction %add-line (text)
  (push text
        (prevalence-multimaster/system:get-root-object :lines)))


(defun add-line (system text)
  "We need this helper to simplify switching between stores."
  (prevalence-multimaster/system:with-system (system)
    (%add-line text)))


(defun get-lines (store)
  (cl-prevalence:get-root-object store :lines))


(define-transaction %add-timestamp ()
  (push (get-universal-time)
        (prevalence-multimaster/system:get-root-object :timestamps)))


(defun add-timestamp (system)
  "We need this helper to simplify switching between stores."
  (prevalence-multimaster/system:with-system (system)
    (%add-timestamp)))


(defun get-timestamps (store)
  (cl-prevalence:get-root-object store :timestamps))


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
        for transactions = (get-transactions store)
        do (format t "~2&System \"~A\":~%" name)
           (when (> (hash-table-count applied-logs)
                    0)
             (format t "  Applied logs:~%")
             (loop for system-name being the hash-keys in applied-logs
                   using (hash-value logs)
                   do (format t "    ~A:~%" system-name)
                      (loop for filename in logs
                            do (format t "      ~A~%" filename))))
           (when transactions
             (format t "  Transactions:~%")
             (loop for tx in transactions
                   do (format t "    ~A~%" tx))))
  (format t "~2&Filesystem:~%")
  (loop for filename in (get-dir-tree "test-m2m")
        do (format t "  ~A~%" filename)))
