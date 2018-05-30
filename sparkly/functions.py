#
# Copyright 2017 Tubular Labs, Inc.
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

from collections import defaultdict
from functools import reduce
import operator

from pyspark.sql import Column
from pyspark.sql import functions as F


def multijoin(dfs, on=None, how=None, coalesce=None):
    """Join multiple dataframes.

    Args:
        dfs (list[pyspark.sql.DataFrame]).
        on: same as ``pyspark.sql.DataFrame.join``.
        how: same as ``pyspark.sql.DataFrame.join``.
        coalesce (list[str]): column names to disambiguate by coalescing
            across the input dataframes. A column must be of the same type
            across all dataframes that define it; if different types appear
            coalesce will do a best-effort attempt in merging them. The
            selected value is the first non-null one in order of appearance
            of the dataframes in the input list. Default is None - don't
            coalesce any ambiguous columns.

    Returns:
        pyspark.sql.DataFrame or None if provided dataframe list is empty.

    Example:
        Assume we have two DataFrames, the first is
        ``first = [{'id': 1, 'value': None}, {'id': 2, 'value': 2}]``
        and the second is
        ``second = [{'id': 1, 'value': 1}, {'id': 2, 'value': 22}]``

        Then collecting the ``DataFrame`` produced by

        ``multijoin([first, second], on='id', how='inner', coalesce=['value'])``

        yields ``[{'id': 1, 'value': 1}, {'id': 2, 'value': 2}]``.
    """
    if not dfs:
        return None

    # Go over the input dataframes and rename each to-be-resolved
    # column to ensure name uniqueness
    coalesce = set(coalesce or [])
    renamed_columns = defaultdict(list)
    for idx, df in enumerate(dfs):
        for col in df.columns:
            if col in coalesce:
                disambiguation = '__{}_{}'.format(idx, col)
                df = df.withColumnRenamed(col, disambiguation)
                renamed_columns[col].append(disambiguation)
        dfs[idx] = df

    # Join the dataframes
    joined_df = reduce(lambda x, y: x.join(y, on=on, how=how), dfs)

    # And coalesce the would-have-been-ambiguities
    for col, disambiguations in renamed_columns.items():
        joined_df = joined_df.withColumn(col, F.coalesce(*disambiguations))
        for disambiguation in disambiguations:
            joined_df = joined_df.drop(disambiguation)

    return joined_df


def switch_case(switch, case=None, default=None, operand=operator.eq, **additional_cases):
    """Switch/case style column generation.

    Args:
        switch (str, pyspark.sql.Column): column to "switch" on;
            its values are going to be compared against defined cases.
        case (dict): case statements. When a key matches the value of
            the column in a specific row, the respective value will be
            assigned to the new column for that row. This is useful when
            your case condition constants are not strings.
        default: default value to be used when the value of the switch
            column doesn't match any keys.
        operand: function to compare the value of the switch column to the
            value of each case. Default is Column's eq. If user-provided,
            first argument will always be the switch Column; it's the
            user's responsibility to transform the case value to a column
            if they need to.
        additional_cases: additional "case" statements, kwargs style.
            Same semantics with cases above. If both are provided,
            cases takes precedence.

    Returns:
        pyspark.sql.Column

    Example:
        ``switch_case('state', CA='California', NY='New York', default='Other')``

        is equivalent to

        >>> F.when(
        ... F.col('state') == 'CA', 'California'
        ).when(
        ... F.col('state') == 'NY', 'New York'
        ).otherwise('Other')

        If you need to "bucketize" a value

        ``switch_case('age', {(13, 17): 1, (18, 24): 2, ...}, operand=lambda c, v: c.between(*v))``

        is equivalent to

        >>> F.when(
        ... F.col('age').between(13, 17), F.lit(1)
        ).when(
        ... F.col('age').between(18, 24), F.lit(2)
        )
    """
    if not isinstance(switch, Column):
        switch = F.col(switch)

    def _column_or_lit(x):
        return F.lit(x) if not isinstance(x, Column) else x

    def _execute_case(accumulator, case):
        # transform the case to a pyspark.sql.functions.when statement,
        # then chain it to existing when statements
        condition_constant, assigned_value = case
        when_args = (operand(switch, condition_constant), _column_or_lit(assigned_value))
        return accumulator.when(*when_args)


    cases = case or {}
    for conflict in set(cases.keys()) & set(additional_cases.keys()):
        del additional_cases[conflict]
    cases = list(cases.items()) + list(additional_cases.items())

    default = _column_or_lit(default)

    if not cases:
        return default

    result = reduce(_execute_case, cases, F).otherwise(default)

    return result
