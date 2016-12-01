Schema management
=================

This package contains utilities for converting string to spark schema definition.
This might be useful for:
 - Specifying schema as (command line) parameter.
 - More convenient interface for specifying schema by hands.


Use cases
^^^^^^^^^

Init Dataframe from data
------------------------

**Why**: Sometimes you know the schema of the data,
but format is not recognized by spark. Then you can
read it as raw python data and apply the known schema to it.
Sparkly utility will make schema definition easy and not hardcoded.

**For example**: You have custom format file without any type information, but
types could be are easily derived.

.. code-block:: python

    from sparkly.schema_parser import generate_structure_type, parse_schema

    data = ... parse data from file ...
    schema_as_string = 'name:string|age:int'  # Note: you can get this from command line, for example
    spark_schema = generate_structure_type(parse_schema(schema_as_string))
    df = ctx.createDataframe(data, spark_schema)

.. automodule:: sparkly.schema_parser
    :members:
