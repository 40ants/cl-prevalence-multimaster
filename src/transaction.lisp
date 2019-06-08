(defpackage #:prevalence-multimaster/transaction
  (:use #:cl)
  (:import-from #:prevalence-multimaster/system
                #:*system*)
  (:import-from #:cl-prevalence
                #:execute-transaction)
  (:import-from #:alexandria
                #:with-gensyms
                #:symbolicate)
  (:import-from #:cl-mock
                #:answer
                #:with-mocks)
  (:export
   #:define-transaction))
(in-package prevalence-multimaster/transaction)


(defvar *in-transaction* nil
  "This variable is used to prevent running one transaction inside of another.
   Because doing this will make both transaction store in the transaction-log.xml
   and this will cause problems later when the log will be replayed.")


(defmacro define-transaction (name (&rest args) &body body)
  "Defines a function to execute transaction and a function to call it.

Additional variable \"system\" will be bound to the current cl-prevalence
store during execution of the \"body\".

Also, a helper defined to call the transaction on hacrm/db::*system*."
  
  (with-gensyms (prevalence-system transaction-timestamp)
    `(eval-when (:compile-toplevel :load-toplevel :execute)
       (defun ,(symbolicate "TX-" name)
           (,prevalence-system ,transaction-timestamp ,@args)
         (declare (ignorable ,prevalence-system))

         ;; When cl-prevalence resystems data from transaction log
         ;; our system can be still a nil and transaction expecting
         ;; it to be a prevalence system, will fail.
         (let ((*system* ,prevalence-system))
           (#+sbcl sb-ext:with-unlocked-packages ;; Without unlock, SBCL prohibits redefinition of the get-universal-time
            #+sbcl(cl)
            #-progn
            (with-mocks (:recordp nil)
              (answer get-universal-time ,transaction-timestamp)
              ,@body))))
       
       (defun ,name (,@args)
         (unless (boundp '*system*)
           (error "Use with-system macro around the code, to bind current *system* variable."))
         (when *in-transaction*
           (error "You are running transaction ~S inside ~S. This can disrupt disrupt the space-time continuum. Please, dont to this!"
                  ',name
                  *in-transaction*))
         
         (let ((*in-transaction* ',name))
           (execute-transaction
            (,(symbolicate "TX-" name)
             *system*
             (get-universal-time)
             ,@args)))))))
