.. sparkle documentation master file, created by
   sphinx-quickstart on Tue Sep 20 08:46:42 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to sparkle's documentation!
===================================

Contents:

.. toctree::
   :maxdepth: 2

   read
   write
   hql
   schema_parser
   utils
   exceptions
   test

.. automodule:: sparkle
   :members:

.. _control-parallelism:

How do we control parallelism?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

As you might notice, all our readers and writers have parameter `parallelism`.
The option is designed to control a load on a source we write to / read from.
It's especially useful when you are working with data storages like Cassandra, MySQL or Elastic.
However, the implementation of the throttling has some drawbacks and you should be aware of them.

The way we implemented it is pretty simple: we use `coalesce` on a dataframe
to reduce an amount of tasks that will be executed in parallel.
Let's say you have a dataframe with 1000 splits and you want to write no more than 10 task
in parallel. In such case `coalesce` will create a dataframe that has 10 splits
with 100 original tasks in each. An outcome of this: if any of these 100 tasks fails,
we have to retry the whole pack in 100 tasks.

`Read more about coalesce <http://spark.apache.org/docs/latest/programming-guide.html#CoalesceLink>`_

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
