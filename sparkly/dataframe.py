#
# Copyright 2021 Tubular Labs, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from functools import reduce

import pyspark
from pyspark.sql import functions as F
from pyspark.sql import types as T

from sparkly import functions as SF


def shape(df):
    return (df.count(), len(df.columns))


def rename(df, mapper=None, **kwargs_mapper):
    """Rename column names of a dataframe

    Args:
        mapper (dict): a dict mapping from the old column names to the new names
        **kwargs_mapper: mapping from the old column names to the new names

    Return:
        pyspark.sql.DataFrame

    Example:
        df = df.rename({'old_col_name': 'new_col_name', 'old_col_name2': 'new_col_name2'})
        df = df.rename(old_col_name=new_col_name, old_col_name2=new_col_name_2)
    """
    if mapper:
        for before, after in mapper.items():
            df = df.withColumnRenamed(before, after)
    for before, after in kwargs_mapper.items():
        df = df.withColumnRenamed(before, after)
    return df


def check(df, n=5, columns=None, to_pandas=False, **kwargs):
    """Quick check on a dataframe with the count information

    Show the first n rows with count and number of columns to make sure your dataframe is what you
    expected. This can also be used to trigger the `.cache()` of dataframe as well.

    Args:
        n (int): numbers of top rows to show. If using `to_pandas=True`, it will also show the
            tail n rows.
        columns (int): max columns to show when `to_pandas=True`
        to_pandas (bool): display as a pandas dataframe in ipython. Require ipython installed.
        **kwargs: additional keyword arguments for `pyspark.sql.DataFrame.show()`

    Example:
        >>> df.check(2)
        shape = (12345, 2)
        +---+---+
        |  x|  y|
        +---+---+
        | 20| 80|
        |  5| 12|
        +---+---+
    """
    print(f'shape = ({df.count()}, {len(df.columns)})')
    if not to_pandas:
        df.show(n, **kwargs)
        return

    from IPython.display import display
    import pandas as pd
    df = df.limit(n).toPandas()
    split = len(df) > 2 * n
    df_show = pd.concat([df.head(n), df.tail(n)]) if split else df
    with pd.option_context('display.max_rows', 2 * n):
        if columns is not None:
            with pd.option_context('display.max_columns', columns):
                display(df_show)
        else:
            display(df_show)


def filters(df, *conditions, **cols_equals):
    """Multiples `where` conditions in one call

    Multilples conditions are connected by `&` operation. When using the keyward arguments
    `cols_equals`, it assumes equal sign. If the value is a `list` or a `set`, it perform
    `.isin(values)`. If the value is a `tuple`, it perform `.between(*values)`.

    Alias: wheres

    Args:
        *conditions (pyspark.sql.Column, str): filter conditions or expression that all need to be
            true
        **cols_equals: equal condition filter, where key is the column name and value is the
            target value. If the value is a list or set, it assumes `.isin(value)`. If the value is
            a tuple, it assumes `.between(*value)`.

    Return:
        pyspark.sql.DataFrame

    Examples:
        df.filters(F.col('views') > 3, F.col('platform') == 'youtube')
        df.filters(platform='youtube', month=('2020-01', '2020-04'), threshold=[-1, 3, 30])
        df.filters(threshold=[0, 3, 5])
        is the same as df.where(F.col('threshold').isin([0, 3, 5]))
        df.filters(views=(2, 10))
        is the same as df.where(F.col('views').between(2, 10))
    """
    conditions = [F.expr(c) if isinstance(c, str) else c for c in conditions]
    conditions = conditions + [
        F.col(col).isin(value) if isinstance(value, (list, set)) else
        F.col(col).between(*value) if isinstance(value, tuple) else
        F.col(col) == value
        for col, value in cols_equals.items()
    ]
    return df.where(reduce(lambda x, y: x & y, conditions))


wheres = filters


def sum_col(df, col):
    """Return the sum of a column as a python int or float

    Args:
        col (str, pyspark.sql.Column): a column to sum over

    Return:
        int or float: sum of the column

    Example:
        >>> df = spark.createDataFrame([[1, True], [2, False], [3, True]], schema=['a', 'b'])
        >>> df.show()
        +---+-----+
        |  a|    b|
        +---+-----+
        |  1| True|
        |  2|False|
        |  3| True|
        +---+-----+
        >>> df.sum_col('a')
        6
        >>> df.sum_col('b')
        2
    """
    if isinstance(col, str):
        if df.schema[col].dataType == T.BooleanType():
            col = F.col(col).astype(T.IntegerType())
    return df.select(F.sum(col)).collect()[0][0]


def mean_col(df, col):
    """Return the mean of a column as a python int or float

    Args:
        col (str, pyspark.sql.Column): a column to average over

    Return:
        float: mean of the column

    Example:
        df.mean_col('a')
    """
    if isinstance(col, str):
        if df.schema[col].dataType == T.BooleanType():
            col = F.col(col).astype(T.IntegerType())
    return df.select(F.mean(col)).collect()[0][0]


def sum_cols(df, cols, col_out):
    """Add a sum of multiple columns of a dataframe

    Args:
        cols (list, tuple[str, pyspark.sql.Column]): columns to sum over

    Returns:
        pyspark.sql.DataFrame

    Example:
        >>> df = spark.createDataFrame([[1, 2, 3], [0, 1, 0]], schema=['a', 'b', 'c'])
        >>> df.show()
        +---+---+---+
        |  a|  b|  c|
        +---+---+---+
        |  1|  2|  3|
        |  0|  1|  0|
        +---+---+---+
        df = df.sum_cols(['a', 'b', 'c'], 'sum')
        >>> df.show()
        +---+---+---+---+
        |  a|  b|  c|sum|
        +---+---+---+---+
        |  1|  2|  3|  6|
        |  0|  1|  0|  1|
        +---+---+---+---+
    """
    return df.withColumn(col_out, SF.sum_cols(cols))


def mean_cols(df, cols, col_out):
    """Add a mean of multiple columns of a dataframe

    Args:
        cols (list, tuple[str, pyspark.sql.Column]): columns to average over

    Returns:
        pyspark.sql.DataFrame

    Example:
        df = spark.createDataFrame()
        df = df.mean_cols(['a', 'b', 'c'], 'mean')
    """
    return df.withColumn(col_out, SF.mean_cols(cols))


def install_extension():
    pyspark.sql.dataframe.DataFrame.shape = shape
    pyspark.sql.dataframe.DataFrame.rename = rename
    pyspark.sql.dataframe.DataFrame.check = check
    pyspark.sql.dataframe.DataFrame.filters = filters
    pyspark.sql.dataframe.DataFrame.wheres = filters
    pyspark.sql.dataframe.DataFrame.sum_col = sum_col
    pyspark.sql.dataframe.DataFrame.mean_col = mean_col
    pyspark.sql.dataframe.DataFrame.sum_cols = sum_cols
    pyspark.sql.dataframe.DataFrame.mean_cols = mean_cols
    pyspark.sql.column.Column.abs = SF.abs
