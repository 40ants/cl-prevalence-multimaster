(defsystem prevalence-multimaster-test
           :author "Alexander Artemenko"
           :license "BSD"
           :class :package-inferred-system
           :pathname "t"
           :depends-on ("prevalence-multimaster-test/core")
           :description "Test system for prevalence-multimaster"

           :perform (test-op :after (op c)
                             (symbol-call :rove :run c)
                             (clear-system c)))
