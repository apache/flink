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

from pyflink.table.types import DataType as TableDataType, DataTypes
from pyflink.util.api_stability_decorators import PublicEvolving

__all__ = ["DataType"]


@PublicEvolving()
class DataType:
    """
    Describes the logical data type of a value in the DataFrame API.

    Data types declare the types of values used by DataFrame expressions and operations.

    Example::

        >>> import pyflink.dataframe as pf
        >>> integer_type = pf.DataType.int64()
        >>> string_type = pf.DataType.string()

    .. versionadded:: 2.4.0
    """

    def __init__(self, table_data_type: TableDataType):
        self._table_data_type = table_data_type

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DataType):
            return False
        return self._table_data_type == other._table_data_type

    def __hash__(self) -> int:
        return hash(str(self._table_data_type))

    @classmethod
    @PublicEvolving()
    def int64(cls) -> "DataType":
        """
        Create a 64-bit integer type.

        Example::

            >>> import pyflink.dataframe as pf
            >>> expression = pf.lit(42, pf.DataType.int64())

        .. versionadded:: 2.4.0
        """
        return cls(DataTypes.BIGINT())

    @classmethod
    @PublicEvolving()
    def string(cls) -> "DataType":
        """
        Create a variable-length string type.

        Example::

            >>> import pyflink.dataframe as pf
            >>> expression = pf.lit("Alice", pf.DataType.string())

        .. versionadded:: 2.4.0
        """
        return cls(DataTypes.STRING())

    def _to_table_data_type(self) -> TableDataType:
        return self._table_data_type
