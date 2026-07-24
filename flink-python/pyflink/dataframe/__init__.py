################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

"""
A DataFrame API for PyFlink.

It provides a fluent interface for composing and executing data transformations.

Example::

    >>> import pyflink.dataframe as pf
    >>> df = pf.from_records(
    ...     [(1, "Alice", 30), (2, "Bob", 17)],
    ...     schema=["id", "name", "age"],
    ... )
    >>> result = (
    ...     df.filter(pf.col("age") >= 18)
    ...     .with_column("age_next_year", pf.col("age") + 1)
    ...     .select("id", "name", "age_next_year")
    ... )
    >>> for row in result.collect():
    ...     print(row)
    <Row(1, 'Alice', 31)>
"""

from pyflink.dataframe.convert import from_dict, from_records
from pyflink.dataframe.context import (
    get_or_create_table_environment,
    get_table_environment,
    set_table_environment,
)
from pyflink.dataframe.dataframe import DataFrame, col, lit
from pyflink.dataframe.datatype import DataType

__all__ = [
    "DataFrame",
    "DataType",
    "col",
    "lit",
    "from_dict",
    "from_records",
    "set_table_environment",
    "get_table_environment",
    "get_or_create_table_environment",
]
