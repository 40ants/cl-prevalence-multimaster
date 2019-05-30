(defpackage #:prevalence-multimaster-test/core
  (:use #:cl
        #:rove
        #:hamcrest/rove)
  (:import-from #:prevalence-multimaster/system
                #:get-applied-logs
                #:make-system)
  (:import-from #:prevalence-multimaster-test/utils
                #:matches
                #:get-lines
                #:add-line)
  (:import-from #:prevalence-multimaster/sync
                #:sync-with-other-masters))
(in-package prevalence-multimaster-test/core)


(deftest test-sync-of-two-systems
  (testing "Checking if lines will appear in both systems after two syncs."
    (osicat:delete-directory-and-files "test-m2m" :if-does-not-exist :ignore)
    (let ((store-a (make-system "test-m2m" "a"))
          (store-b (make-system "test-m2m" "b")))
      (add-line store-a "A1")
      (add-line store-a "A2")
      (add-line store-b "B1")
      (add-line store-b "B2")
      (assert-that (get-lines store-a)
                   (contains "A2" "A1"))
      (assert-that (get-lines store-b)
                   (contains "B2" "B1"))

      (sync-with-other-masters store-a)
      (sync-with-other-masters store-b)
      (sync-with-other-masters store-a)
      (sync-with-other-masters store-b)

      (assert-that (get-lines store-a)
                   (contains  "B2" "B1" "A2" "A1"))
      (assert-that (get-lines store-b)
                   (contains "A2" "A1" "B2" "B1")))))


(deftest test-new-system-pickup-data-from-old-one
  (testing "Checking if a fresh system will load data from some other master."
    (osicat:delete-directory-and-files "test-m2m" :if-does-not-exist :ignore)
    (let ((store-a (make-system "test-m2m" "a")))
      (add-line store-a "A1")
      (add-line store-a "A2")

      ;; Now we need to create a snapshot for first system
      (sync-with-other-masters store-a)

      (testing "Now system A should remember a generated transaction log's snapshot."
        (assert-that (get-applied-logs :system store-a)
                     (contains
                      (matches "a/transaction-log-.*.xml"))))

      ;; And to generate a transaction log on system A
      (add-line store-a "A3")
      (add-line store-a "A4")

      (let ((store-b (make-system "test-m2m" "b")))
        (testing "System B after restoring A's state should also thinks it is already applied the log."
          (assert-that (get-applied-logs :system store-b)
                       (contains
                        (matches "a/transaction-log-.*.xml"))))
        
        (testing "After creation, system b should pickup a state of system A from it's lates snapshot"
          (let ((lines (get-lines store-b)))
            (assert-that lines
                         (contains "A2" "A1"))))

        ;; Doing another sync iteration and expecting
        ;; both stores to be in a consitent state
        ;; We should ensure there is 1 or more seconds before syncs,
        ;; because otherwise, transaction log backup will be written
        ;; into the old file
        (sleep 1)
        (sync-with-other-masters store-a)
        (sync-with-other-masters store-b)
        
        (testing "But after a sync, it should to catch up A's state"
          (let ((lines (get-lines store-b)))
            (assert-that lines
                         (contains "A4" "A3" "A2" "A1"))))))))
