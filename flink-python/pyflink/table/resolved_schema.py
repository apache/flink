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
from typing import List, Optional

from pyflink.java_gateway import get_gateway
from pyflink.table.catalog import Column, WatermarkSpec, UniqueConstraint
from pyflink.table.types import DataType, _to_java_data_type, _from_java_data_type
from pyflink.util.java_utils import to_jarray

__all__ = ["ResolvedSchema"]


class ResolvedSchema(object):
    """
    Schema of a table or view consisting of columns, constraints, and watermark specifications.

    This class is the result of resolving a :class:`~pyflink.table.Schema` into a final validated
    representation.

    - Data types and functions have been expanded to fully qualified identifiers.
    - Time attributes are represented in the column's data type.
    - :class:`pyflink.table.Expression` have been translated to
      :class:`pyflink.table.ResolvedExpression`

    This class should not be passed into a connector. It is therefore also not serializable.
    Instead, the :func:`to_physical_row_data_type` can be passed around where necessary.
    """

    def __init__(
        self,
        columns: List[Column] = None,
        watermark_specs: List[WatermarkSpec] = None,
        primary_key: Optional[UniqueConstraint] = None,
        j_resolved_schema=None,
    ):
        if j_resolved_schema is None:
            assert columns is not None
            assert watermark_specs is not None

            gateway = get_gateway()
            j_columns = to_jarray(
                gateway.jvm.org.apache.flink.table.catalog.Column, [c._j_column for c in columns]
            )
            j_watermark_specs = to_jarray(
                gateway.jvm.org.apache.flink.table.catalog.WatermarkSpec,
                [w._j_watermark_spec for w in watermark_specs],
            )
            j_primary_key = primary_key._j_unique_constraint if primary_key is not None else None
            self._j_resolved_schema = gateway.jvm.org.apache.flink.table.catalog.ResolvedSchema(
                j_columns, j_watermark_specs, j_primary_key
            )
        else:
            self._j_resolved_schema = j_resolved_schema

    def __str__(self):
        return self._j_resolved_schema.toString()

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self._j_resolved_schema.equals(
            other._j_resolved_schema
        )

    def __hash__(self):
        return self._j_resolved_schema.hashCode()

    @staticmethod
    def of(columns: List[Column]) -> "ResolvedSchema":
        """
        Shortcut for a resolved schema of only columns.
        """
        gateway = get_gateway()
        j_columns = to_jarray(
            gateway.jvm.org.apache.flink.table.catalog.Column, [c._j_column for c in columns]
        )
        j_resolved_schema = gateway.jvm.org.apache.flink.table.catalog.ResolvedSchema.of(j_columns)
        return ResolvedSchema(j_resolved_schema=j_resolved_schema)

    @staticmethod
    def physical(column_names: List[str], column_data_types: List[DataType]) -> "ResolvedSchema":
        """
        Shortcut for a resolved schema of only physical columns.
        """
        gateway = get_gateway()
        j_col_names = to_jarray(gateway.jvm.String, column_names)
        j_col_data_types = to_jarray(
            gateway.jvm.org.apache.flink.table.types.DataType,
            [_to_java_data_type(c) for c in column_data_types],
        )
        j_resolved_schema = gateway.jvm.org.apache.flink.table.catalog.ResolvedSchema.physical(
            j_col_names, j_col_data_types
        )
        return ResolvedSchema(j_resolved_schema=j_resolved_schema)

    def get_column_count(self) -> int:
        """
        Returns the number of :class:`~pyflink.table.catalog.Column` of this schema.
        """
        return self._j_resolved_schema.getColumnCount()

    def get_columns(self) -> List[Column]:
        """
        Returns all :class:`~pyflink.table.catalog.Column` of this schema.
        """
        j_columns = self._j_resolved_schema.getColumns()
        return [Column._from_j_column(j_column) for j_column in j_columns]

    def get_column_names(self) -> List[str]:
        """
        Returns all column names. It does not distinguish between different kinds of columns.
        """
        return self._j_resolved_schema.getColumnNames()

    def get_column_data_types(self) -> List[DataType]:
        """
        Returns all column data types. It does not distinguish between different kinds of columns.
        """
        j_data_types = self._j_resolved_schema.getColumnDataTypes()
        return [_from_java_data_type(j_data_type) for j_data_type in j_data_types]

    def get_column_by_index(self, column_index: int) -> Optional[Column]:
        """
        Returns the :class:`~pyflink.table.catalog.Column` instance for the given column index.

        :param column_index: the index of the column
        """
        optional_result = self._j_resolved_schema.getColumn(column_index)
        return Column._from_j_column(optional_result.get()) if optional_result.isPresent() else None

    def get_column_by_name(self, column_name: str) -> Optional[Column]:
        """
        Returns the :class:`~pyflink.table.catalog.Column` instance for the given column index.

        :param column_index: the index of the column
        """
        optional_result = self._j_resolved_schema.getColumn(column_name)
        return Column._from_j_column(optional_result.get()) if optional_result.isPresent() else None

    def get_watermark_specs(self) -> List[WatermarkSpec]:
        """
        Returns a list of watermark specifications each consisting of a rowtime attribute and
        watermark strategy expression.

        Note: Currently, there is at most one :class:`~pyflink.table.catalog.WatermarkSpec`
        in the list, because we don't support multiple watermark definitions yet.
        """
        j_watermark_specs = self._j_resolved_schema.getWatermarkSpecs()
        return [WatermarkSpec(j_watermark_spec) for j_watermark_spec in j_watermark_specs]

    def get_primary_key(self) -> Optional[UniqueConstraint]:
        """
        Returns the primary key if it has been defined.
        """
        optional_result = self._j_resolved_schema.getPrimaryKey()
        return (
            UniqueConstraint(j_unique_constraint=optional_result.get())
            if optional_result.isPresent()
            else None
        )

    def get_primary_key_indexes(self) -> List[int]:
        """
        Returns the primary key indexes, if any, otherwise returns an empty list.
        """
        return self._j_resolved_schema.getPrimaryKeyIndexes()

    def to_source_row_data_type(self) -> DataType:
        """
        Converts all columns of this schema into a (possibly nested) row data type.

        This method returns the **source-to-query schema**.

        Note: The returned row data type contains physical, computed, and metadata columns. Be
        careful when using this method in a table source or table sink. In many cases,
        :func:`to_physical_row_data_type` might be more appropriate.
        """
        j_data_type = self._j_resolved_schema.toSourceRowDataType()
        return _from_java_data_type(j_data_type)

    def to_physical_row_data_type(self) -> DataType:
        """
        Converts all physical columns of this schema into a (possibly nested) row data type.

        Note: The returned row data type contains only physical columns. It does not include
        computed or metadata columns.
        """
        j_data_type = self._j_resolved_schema.toPhysicalRowDataType()
        return _from_java_data_type(j_data_type)

    def to_sink_row_data_type(self):
        """
        Converts all persisted columns of this schema into a (possibly nested) row data type.

        This method returns the **query-to-sink schema**.

        Note: Computed columns and virtual columns are excluded in the returned row data type. The
        data type contains the columns of :func:`to_physical_row_data_type` plus persisted metadata
        columns.
        """
        j_data_type = self._j_resolved_schema.toSinkRowDataType()
        return _from_java_data_type(j_data_type)
