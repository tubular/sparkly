Schema management
=================

This package contains utilities for converting string to spark schema definition.
This might be useful for:
 - Specifying schema as (command line) parameter.
 - More convenient interface for specifying schema by hands.


Use cases
---------

Force custom schema for a dataframe
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Why**: Sometimes you know the schema of the data,
but format is not recognized by spark. Then you can
read it as raw python data and apply the known schema to it.
Sparkly utility will make schema definition easy and not hardcoded.

**For example**: You have custom format file without any type information, but
types could be are easily derived.

.. code-block:: python

    from sparkly import schema_parser

    rows = ... parse rows from a file ...
    schema_as_string = 'name:string|age:int'  # You can pass the schema as a command line argument.
    dataframe_schema = schema_parser.parse(schema_as_string)
    df = spark.createDataFrame(rows, dataframe_schema)

.. automodule:: sparkly.schema_parser
    :members:
