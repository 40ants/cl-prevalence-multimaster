========================
 prevalence-multimaster
========================

.. insert-your badges like that:

.. image:: https://travis-ci.org/40ants/prevalence-multimaster.svg?branch=master
    :target: https://travis-ci.org/40ants/prevalence-multimaster

.. Everything starting from this commit will be inserted into the
   index page of the HTML documentation.
.. include-from

This system contains an addon which allows to syncronize multiple
cl-prevalence systems state.

This is useful when you have apps running on multiple machines and want
them to have eventually consistent state.

However, there are two caveat:

* Consistency is evential.
* In some cases consistency could not be reached.

How does this work
==================

1. First thing you need to provide for all app instances is a folder where
   they will store their prevalence databases and logs.

   This folder can be inside of a Dropbox, for example.

   Eventually, each app should see all other app's transaction logs.

2. When you call a ``sync-with-other-masters`` on the prevalence system,
   it does few things:

   * makes a snapshot of it database;
   * searches not processed transaction logs from other masters;
   * and executes transaction from each found log;
   * removes himself logs, processed by all other masters.

   .. warning:: Right now ``sync-with-other-masters`` should be called
                not often then once per second, because otherwise filenames
                of transaction log backups will clash.

Roadmap
=======

1. Ensure this will work on the real world application.
2. Support old transaction logs deletion after they was processed by all apps.
2. Add a documentation.
3. ...

.. Everything after this comment will be omitted from HTML docs.
.. include-to

Building Documentation
======================

Provide instruction how to build or use your library.

How to build documentation
--------------------------

To build documentation, you need a Sphinx. It is
documentaion building tool written in Python.

To install it, you need a virtualenv. Read
this instructions
`how to install it
<https://virtualenv.pypa.io/en/stable/installation/#installation>`_.

Also, you'll need a `cl-launch <http://www.cliki.net/CL-Launch>`_.
It is used by documentation tool to run a script which extracts
documentation strings from lisp systems.

Run these commands to build documentation::

  virtualenv --python python2.7 env
  source env/bin/activate
  pip install -r docs/requirements.txt
  invoke build_docs

These commands will create a virtual environment and
install some python libraries there. Command ``invoke build_docs``
will build documentation and upload it to the GitHub, by replacing
the content of the ``gh-pages`` branch.

