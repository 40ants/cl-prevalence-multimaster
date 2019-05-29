(defpackage #:prevalence-multimaster/transaction
  (:use #:cl)
  (:import-from #:prevalence-multimaster/system
                #:*system*)
  (:import-from #:cl-prevalence
                #:execute-transaction)
  (:import-from #:alexandria
                #:with-gensyms
                #:symbolicate)
  (:export
   #:define-transaction))
(in-package prevalence-multimaster/transaction)


(defmacro define-transaction (name (&rest args) &body body)
  "Defines a function to execute transaction and a function to call it.

Additional variable \"system\" will be bound to the current cl-prevalence
store during execution of the \"body\".

Also, a helper defined to call the transaction on hacrm/db::*system*."
  
  (with-gensyms (prevalence-system)
    `(eval-when (:compile-toplevel :load-toplevel :execute)
       (defun ,(symbolicate "TX-" name) (,prevalence-system ,@args)
         (declare (ignorable ,prevalence-system))

         ;; When cl-prevalence resystems data from transaction log
         ;; our system can be still a nil and transaction expecting
         ;; it to be a prevalence system, will fail.
         (let ((*system* ,prevalence-system))
           ,@body))
          
       (defun ,name (,@args)
         (unless (boundp '*system*)
           (error "Use with-system macro around the code, to bind current *system* variable."))
         (execute-transaction
          (,(symbolicate "TX-" name) *system* ,@args))))))
