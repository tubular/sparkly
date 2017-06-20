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

from functools import reduce

from pyspark.sql import Column
from pyspark.sql import functions as F


def switch_case(switch, case=None, default=None, **additional_cases):
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
    """
    if not isinstance(switch, Column):
        switch = F.col(switch)

    def _column_or_lit(x):
        return F.lit(x) if not isinstance(x, Column) else x

    def _execute_case(accumulator, case):
        # transform the case to a pyspark.sql.functions.when statement,
        # then chain it to existing when statements
        condition_constant, assigned_value = case
        when_args = (switch == F.lit(condition_constant), _column_or_lit(assigned_value))
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
