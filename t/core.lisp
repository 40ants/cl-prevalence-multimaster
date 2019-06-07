(defpackage #:prevalence-multimaster-test/core
  (:use #:cl
        #:rove
        #:hamcrest/rove)
  (:import-from #:prevalence-multimaster/system
                #:multimaster-system
                #:get-applied-logs
                #:make-system
                #:*system*)
  (:import-from #:osicat
                #:delete-directory-and-files)
  (:import-from #:prevalence-multimaster-test/utils
                #:make-store
                #:dump-state
                #:matches
                #:get-lines
                #:get-dir-tree
                #:add-line)
  (:import-from #:prevalence-multimaster/sync
                #:sync-with-other-masters))
(in-package prevalence-multimaster-test/core)



(deftest test-sync-of-two-systems
  (testing "Checking if lines will appear in both systems after two syncs."
    (delete-directory-and-files "test-m2m" :if-does-not-exist :ignore)
    (let ((store-a (make-store "a"))
          (store-b (make-store "b")))
      (add-line store-a "A1")
      (add-line store-a "A2")
      (add-line store-b "B1")
      (add-line store-b "B2")
      (assert-that (get-lines store-a)
                   (contains "A2" "A1"))
      (assert-that (get-lines store-b)
                   (contains "B2" "B1"))

      (dump-state "Before syncs" store-a store-b)

      ;; We need these ":delete-old-logs nil" here
      ;; because otherwise sychronization process will not
      ;; start.
      (sync-with-other-masters store-a :force t :delete-old-logs nil)
      (dump-state "After first \"a\" sync" store-a store-b)

      (sync-with-other-masters store-b :force t :delete-old-logs nil)
      (dump-state "After first \"b\" sync" store-a store-b)

      (sync-with-other-masters store-a :force t)
      (dump-state "After second \"a\" sync" store-a store-b)

      (sync-with-other-masters store-b :force t)
      (dump-state "After second \"b\" sync" store-a store-b)
      
      (testing "That store \"a\" have all data"
        (assert-that (get-lines store-a)
                     (contains  "B2" "B1" "A2" "A1")))
      (testing "That store \"b\" have all data"
        (assert-that (get-lines store-b)
                     (contains "A2" "A1" "B2" "B1"))))))


(deftest test-new-system-pickup-data-from-old-one
  (testing "Checking if a fresh system will load data from some other master."
    (delete-directory-and-files "test-m2m" :if-does-not-exist :ignore)
    (let ((store-a (make-store "a")))
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

      (let ((store-b (make-store "b")))
        (testing "System B after restoring A's state should also thinks it is already applied the log."
          (assert-that (get-applied-logs :system store-b)
                       (contains
                        (matches "a/transaction-log-.*.xml"))))
        
        (testing "After creation, system b should pickup a state of system A from it's lates snapshot"
          (let ((lines (get-lines store-b)))
            (assert-that lines
                         (contains "A2" "A1"))))

        ;; Doing another sync iteration and expecting
        ;; both stores to be in a consistent state
        ;; We should ensure there is 1 or more seconds before syncs,
        ;; because otherwise, transaction log backup will be written
        ;; into the old file
        (dump-state "Before syncs" store-a store-b)
        
        (sync-with-other-masters store-a)
        (dump-state "After \"a\" sync" store-a store-b)
        
        (sync-with-other-masters store-b)
        (dump-state "After \"b\" sync" store-a store-b)

        (testing "But after a sync, it should to catch up A's state"
          (let ((lines (get-lines store-b)))
            (assert-that lines
                         (contains "A4" "A3" "A2" "A1"))))))))


(deftest test-delete-synced-logs
  (testing "Old transaction logs should be removed from disk and :applied-logs."
    (delete-directory-and-files "test-m2m" :if-does-not-exist :ignore)
    (let ((store-a (make-store "a"))
          (store-b (make-store "b")))

      ;; Add some data to make a transaction logs in both stores
      (add-line store-a "A1")
      (add-line store-b "B1")

      ;; Generate snapshots and logs

      (loop repeat 10
            do (sync-with-other-masters store-a :delete-old-logs nil)
               (sync-with-other-masters store-b :delete-old-logs nil))
      (dump-state "after 3 iteration" store-a store-b)

      (format t "~2&TO DELETE IN a:~%")
      (let ((files (prevalence-multimaster/sync::get-files-to-delete store-a)))
        (loop for name in files
              do (format t "~A~%" name)))
      
      (format t "~2&TO DELETE IN b:~%")
      (let ((files (prevalence-multimaster/sync::get-files-to-delete store-b)))
        (loop for name in files
              do (format t "~A~%" name)))

      (prevalence-multimaster/sync::delete-old-logs store-a)
      (prevalence-multimaster/sync::delete-old-logs store-b)
      
      (testing "Check if old logs were deleted"
        (let ((all-files (get-dir-tree "test-m2m")))
          (ok (= (length all-files)
                 6)))))))


(deftest test-pathname-serialization
  (ok (string= (with-output-to-string (s)
                 (s-serialization:serialize-xml #P"foo" s))
               "<PATHNAME>#P\"foo\"</PATHNAME>"))
  (ok (string= (with-output-to-string (s)
                 (s-serialization:serialize-xml #P"/tmp/foo/bar.txt" s))
               "<PATHNAME>#P\"/tmp/foo/bar.txt\"</PATHNAME>")))


(deftest test-pathname-deserialization
  (ok (equal (with-input-from-string (s "<PATHNAME>#P\"foo\"</PATHNAME>")
               (s-serialization:deserialize-xml s))
             #P"foo"))
  (ok (equal (with-input-from-string (s "<PATHNAME>#P\"/tmp/foo/bar.txt\"</PATHNAME>")
               (s-serialization:deserialize-xml s))
             #P"/tmp/foo/bar.txt")))
