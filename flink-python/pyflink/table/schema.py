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
from typing import Union, List

from pyflink.java_gateway import get_gateway
from pyflink.table import Expression
from pyflink.table.expression import _get_java_expression
from pyflink.table.types import DataType, _to_java_data_type
from pyflink.util.java_utils import to_jarray

__all__ = ['Schema']


class Schema(object):
    """
    Schema of a table or view.

    A schema represents the schema part of a {@code CREATE TABLE (schema) WITH (options)} DDL
    statement in SQL. It defines columns of different kind, constraints, time attributes, and
    watermark strategies. It is possible to reference objects (such as functions or types) across
    different catalogs.

    This class is used in the API and catalogs to define an unresolved schema that will be
    translated to ResolvedSchema. Some methods of this class perform basic validation, however, the
    main validation happens during the resolution. Thus, an unresolved schema can be incomplete and
    might be enriched or merged with a different schema at a later stage.

    Since an instance of this class is unresolved, it should not be directly persisted. The str()
    shows only a summary of the contained objects.
    """

    def __init__(self, j_schema):
        self._j_schema = j_schema

    @staticmethod
    def new_builder() -> 'Schema.Builder':
        gateway = get_gateway()
        j_builder = gateway.jvm.Schema.newBuilder()
        return Schema.Builder(j_builder)

    def __str__(self):
        return self._j_schema.toString()

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self._j_schema.equals(other._j_schema)

    def __hash__(self):
        return self._j_schema.hashCode()

    class Builder(object):
        """
        A builder for constructing an immutable but still unresolved Schema.
        """

        def __init__(self, j_builder):
            self._j_builder = j_builder

        def from_schema(self, unresolved_schema: 'Schema') -> 'Schema.Builder':
            """
            Adopts all members from the given unresolved schema.
            """
            self._j_builder.fromSchema(unresolved_schema._j_schema)
            return self

        def from_row_data_type(self, data_type: DataType) -> 'Schema.Builder':
            """
            Adopts all fields of the given row as physical columns of the schema.
            """
            self._j_builder.fromRowDataType(_to_java_data_type(data_type))
            return self

        def from_fields(self,
                        field_names: List[str],
                        field_data_types: List[DataType]) -> 'Schema.Builder':
            """
            Adopts the given field names and field data types as physical columns of the schema.
            """
            gateway = get_gateway()
            j_field_names = to_jarray(gateway.jvm.String, field_names)
            j_field_data_types = to_jarray(
                gateway.jvm.AbstractDataType,
                [_to_java_data_type(field_data_type) for field_data_type in field_data_types])
            self._j_builder.fromFields(j_field_names, j_field_data_types)
            return self

        def column(self,
                   column_name: str,
                   data_type: Union[str, DataType]) -> 'Schema.Builder':
            """
            Declares a physical column that is appended to this schema.

            Physical columns are regular columns known from databases. They define the names, the
            types, and the order of fields in the physical data. Thus, physical columns represent
            the payload that is read from and written to an external system. Connectors and formats
            use these columns (in the defined order) to configure themselves. Other kinds of columns
            can be declared between physical columns but will not influence the final physical
            schema.

            :param column_name: Column name
            :param data_type: Data type of the column
            """
            if isinstance(data_type, str):
                self._j_builder.column(column_name, data_type)
            else:
                self._j_builder.column(column_name, _to_java_data_type(data_type))
            return self

        def column_by_expression(self,
                                 column_name: str,
                                 expr: Union[str, Expression]) -> 'Schema.Builder':
            """
            Declares a computed column that is appended to this schema.

            Computed columns are virtual columns that are generated by evaluating an expression
            that can reference other columns declared in the same table. Both physical columns and
            metadata columns can be accessed. The column itself is not physically stored within the
            table. The column’s data type is derived automatically from the given expression and
            does not have to be declared manually.

            Computed columns are commonly used for defining time attributes. For example, the
            computed column can be used if the original field is not TIMESTAMP(3) type or is nested
            in a JSON string.

            Example:
            ::

                >>> Schema.new_builder().
                ...  column_by_expression("ts", "orig_ts - INTERVAL '60' MINUTE").
                ...  column_by_metadata("orig_ts", DataTypes.TIMESTAMP(3), "timestamp")

            :param column_name: Column name
            :param expr: Computation of the column
            """
            self._j_builder.columnByExpression(column_name, _get_java_expression(expr))
            return self

        def column_by_metadata(self,
                               column_name: str,
                               data_type: Union[DataType, str],
                               metadata_key: str = None,
                               is_virtual: bool = False) -> 'Schema.Builder':
            """
            Declares a metadata column that is appended to this schema.

            Metadata columns allow to access connector and/or format specific fields for every row
            of a table. For example, a metadata column can be used to read and write the timestamp
            from and to Kafka records for time-based operations. The connector and format
            documentation lists the available metadata fields for every component.

            Every metadata field is identified by a string-based key and has a documented data
            type. The metadata key can be omitted if the column name should be used as the
            identifying metadata key. For convenience, the runtime will perform an explicit cast if
            the data type of the column differs from the data type of the metadata field. Of course,
            this requires that the two data types are compatible.

            By default, a metadata column can be used for both reading and writing. However, in
            many cases an external system provides more read-only metadata fields than writable
            fields. Therefore, it is possible to exclude metadata columns from persisting by setting
            the {@code is_virtual} flag to {@code true}.

            :param column_name: Column name
            :param data_type: Data type of the column
            :param metadata_key: Identifying metadata key, if null the column name will be used as
                metadata key
            :param is_virtual: Whether the column should be persisted or not
            """
            if isinstance(data_type, DataType):
                self._j_builder.columnByMetadata(
                    column_name,
                    _to_java_data_type(data_type),
                    metadata_key, is_virtual)
            else:
                self._j_builder.columnByMetadata(
                    column_name,
                    data_type,
                    metadata_key,
                    is_virtual)
            return self

        def watermark(self,
                      column_name: str,
                      watermark_expr: Union[str, Expression]) -> 'Schema.Builder':
            """
            Declares that the given column should serve as an event-time (i.e. rowtime) attribute
            and specifies a corresponding watermark strategy as an expression.

            The column must be of type {@code TIMESTAMP(3)} or {@code TIMESTAMP_LTZ(3)} and be a
            top-level column in the schema. It may be a computed column.

            The watermark generation expression is evaluated by the framework for every record
            during runtime. The framework will periodically emit the largest generated watermark. If
            the current watermark is still identical to the previous one, or is null, or the value
            of the returned watermark is smaller than that of the last emitted one, then no new
            watermark will be emitted. A watermark is emitted in an interval defined by the
            configuration.

            Any scalar expression can be used for declaring a watermark strategy for
            in-memory/temporary tables. However, currently, only SQL expressions can be persisted in
            a catalog. The expression's return data type must be {@code TIMESTAMP(3)}. User-defined
            functions (also defined in different catalogs) are supported.

            Example:
            ::

                >>> Schema.new_builder().watermark("ts", "ts - INTERVAL '5' SECOND")

            :param column_name: The column name used as a rowtime attribute
            :param watermark_expr: The expression used for watermark generation
            """
            self._j_builder.watermark(column_name, _get_java_expression(watermark_expr))
            return self

        def primary_key(self, *column_names: str) -> 'Schema.Builder':
            """
            Declares a primary key constraint for a set of given columns. Primary key uniquely
            identify a row in a table. Neither of columns in a primary can be nullable. The primary
            key is informational only. It will not be enforced. It can be used for optimizations. It
            is the data owner's responsibility to ensure uniqueness of the data.

            The primary key will be assigned a generated name in the format {@code PK_col1_col2}.

            :param column_names: Columns that form a unique primary key
            """
            gateway = get_gateway()
            self._j_builder.primaryKey(to_jarray(gateway.jvm.java.lang.String, column_names))
            return self

        def primary_key_named(self,
                              constraint_name: str,
                              *column_names: str) -> 'Schema.Builder':
            """
            Declares a primary key constraint for a set of given columns. Primary key uniquely
            identify a row in a table. Neither of columns in a primary can be nullable. The primary
            key is informational only. It will not be enforced. It can be used for optimizations. It
            is the data owner's responsibility to ensure uniqueness of the data.

            :param constraint_name: Name for the primary key, can be used to reference the
                constraint
            :param column_names: Columns that form a unique primary key
            """
            gateway = get_gateway()
            self._j_builder.primaryKeyNamed(
                constraint_name,
                to_jarray(gateway.jvm.java.lang.String, column_names))
            return self

        def build(self) -> 'Schema':
            """
            Returns an instance of an unresolved Schema.
            """
            return Schema(self._j_builder.build())
