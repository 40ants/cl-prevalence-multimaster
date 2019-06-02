===========
 ChangeLog
===========

0.2.0 (2019-06-02)
==================

* Fixed issue when system loaded itself logs when it's root is given with HOME, like this:
  ``~/Docker/app/``.

  Now we use ``uiop:truenamize`` instead of ``osicat:absolute-pathname``.

0.1.0 (2019-05-21)
==================

* Initial version.
