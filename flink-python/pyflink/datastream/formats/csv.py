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
from typing import Optional, cast

from pyflink.datastream.connectors import StreamFormat
from pyflink.java_gateway import get_gateway
from pyflink.table.types import DataType, DataTypes, _to_java_data_type, RowType, NumericType


class CsvSchema(object):
    """
    CsvSchema holds schema information of a csv file, corresponding to Java
    ``com.fasterxml.jackson.dataformat.csv.CsvSchema`` class.

    .. versionadded:: 1.16.0
    """

    def __init__(self, j_schema, data_type: DataType):
        self._j_schema = j_schema
        self._data_type = data_type

    @staticmethod
    def builder() -> 'CsvSchemaBuilder':
        """
        Returns a :class:`CsvSchemaBuilder`.
        """
        return CsvSchemaBuilder()

    def size(self):
        return self._j_schema.size()

    def __len__(self):
        return self.size()

    def __str__(self):
        return self._j_schema.toString()

    def __repr__(self):
        return str(self)


class CsvSchemaBuilder(object):
    """
    CsvSchemaBuilder is for building a :class:`CsvSchemaBuilder`, corresponding to Java
    ``com.fasterxml.jackson.dataformat.csv.CsvSchema.Builder`` class.

    .. versionadded:: 1.16.0
    """

    def __init__(self):
        jvm = get_gateway().jvm
        self._j_schema_builder = jvm.org.apache.flink.shaded.jackson2.com.fasterxml.jackson \
            .dataformat.csv.CsvSchema.builder()
        self._fields = []

    def build(self) -> 'CsvSchema':
        """
        Build the :class:`CsvSchema`.
        """
        return CsvSchema(self._j_schema_builder.build(), DataTypes.ROW(self._fields))

    def add_array_column(self,
                         name: str,
                         separator: str = ';',
                         element_type: Optional[DataType] = DataTypes.STRING()) \
            -> 'CsvSchemaBuilder':
        """
        Add an array column to schema, the type of elements could be specified via ``element_type``,
        which should be primitive types.

        :param name: Name of the column.
        :param separator: Text separator of array elements, default to ``;``.
        :param element_type: DataType of array elements, default to ``DataTypes.STRING()``.
        """
        self._j_schema_builder.addArrayColumn(name, separator)
        self._fields.append(DataTypes.FIELD(name, DataTypes.ARRAY(element_type)))
        return self

    def add_boolean_column(self, name: str) -> 'CsvSchemaBuilder':
        """
        Add a boolean column to schema, with type as ``DataTypes.BOOLEAN()``.

        :param name: Name of the column.
        """
        self._j_schema_builder.addBooleanColumn(name)
        self._fields.append(DataTypes.FIELD(name, DataTypes.BOOLEAN()))
        return self

    def add_number_column(self, name: str,
                          number_type: Optional[NumericType] = DataTypes.BIGINT()) \
            -> 'CsvSchemaBuilder':
        """
        Add a number column to schema, the type of number could be specified via ``number_type``.

        :param name: Name of the column.
        :param number_type: DataType of the number, default to ``DataTypes.BIGINT()``.
        """
        self._j_schema_builder.addNumberColumn(name)
        self._fields.append(DataTypes.FIELD(name, number_type))
        return self

    def add_string_column(self, name: str) -> 'CsvSchemaBuilder':
        """
        Add a string column to schema, with type as ``DataTypes.STRING()``.

        :param name: Name of the column.
        """
        self._j_schema_builder.addColumn(name)
        self._fields.append(DataTypes.FIELD(name, DataTypes.STRING()))
        return self

    def add_columns_from(self, schema: 'CsvSchema') -> 'CsvSchemaBuilder':
        """
        Add all columns in ``schema`` to current schema.

        :param schema: Another :class:`CsvSchema`.
        """
        self._j_schema_builder.addColumnsFrom(schema._j_schema)
        for field in cast(schema._data_type, RowType):
            self._fields.append(field)
        return self

    def clear_columns(self):
        """
        Delete all columns in the schema.
        """
        self._j_schema_builder.clearColumns()
        self._fields.clear()
        return self

    def set_allow_comments(self, allow: bool = True):
        """
        Allow using ``#`` prefixed comments in csv file.
        """
        self._j_schema_builder.setAllowComments(allow)
        return self

    def set_any_property_name(self, name: str):
        self._j_schema_builder.setAnyPropertyName(name)
        return self

    def disable_array_element_separator(self):
        """
        Set array element separator to ``""``.
        """
        self._j_schema_builder.disableArrayElementSeparator()
        return self

    def remove_array_element_separator(self, index: int):
        """
        Set array element separator of a column specified by ``index`` to ``""``.
        """
        self._j_schema_builder.removeArrayElementSeparator(index)
        return self

    def set_array_element_separator(self, separator: str):
        """
        Set global array element separator, default to ``;``.
        """
        self._j_schema_builder.setArrayElementSeparator(separator)
        return self

    def set_column_separator(self, char: str):
        """
        Set column separator, ``char`` should be a single char, default to ``,``.
        """
        if len(char) != 1:
            raise ValueError('Column separator must be a single char, got {}'.format(char))
        self._j_schema_builder.setColumnSeparator(char)
        return self

    def disable_escape_char(self):
        """
        Disable escaping in csv file.
        """
        self._j_schema_builder.disableEscapeChar()
        return self

    def set_escape_char(self, char: str):
        """
        Set escape char, ``char`` should be a single char, default to no-escaping.
        """
        if len(char) != 1:
            raise ValueError('Escape char must be a single char, got {}'.format(char))
        self._j_schema_builder.setEscapeChar(char)
        return self

    def set_line_separator(self, separator: str):
        """
        Set line separator, default to ``\\n``. This is only configurable for writing, for reading,
        ``\\n``, ``\\r``, ``\\r\\n`` are recognized.
        """
        self._j_schema_builder.setLineSeparator(separator)
        return self

    def set_null_value(self, null_value: str):
        """
        Set literal for null value, default to empty sequence.
        """
        self._j_schema_builder.setNullValue(null_value)

    def disable_quote_char(self):
        """
        Disable quote char.
        """
        self._j_schema_builder.disableQuoteChar()
        return self

    def set_quote_char(self, char: str):
        """
        Set quote char, default to ``"``.
        """
        if len(char) != 1:
            raise ValueError('Quote char must be a single char, got {}'.format(char))
        self._j_schema_builder.setQuoteChar(char)
        return self

    def set_skip_first_data_row(self, skip: bool = True):
        """
        Set whether to skip the first row of csv file.
        """
        self._j_schema_builder.setSkipFirstDataRow(skip)
        return self

    def set_strict_headers(self, strict: bool = True):
        """
        Set whether to use strict headers, which check column names in the header are consistent
        with the schema.
        """
        self._j_schema_builder.setStrictHeaders(strict)
        return self

    def set_use_header(self, use: bool = True):
        """
        Set whether to read header.
        """
        self._j_schema_builder.setUseHeader(use)
        return self

    def size(self):
        return len(self._fields)

    def __len__(self):
        return self.size()


class CsvReaderFormat(StreamFormat):
    """
    The :class:`StreamFormat` for reading csv files.

    Example:
    ::

        >>> schema = CsvSchema.builder() \\
        ...     .add_number_column('id', number_type=DataTypes.INT()) \\
        ...     .add_string_column('name') \\
        ...     .add_array_column('list', ',', element_type=DataTypes.STRING()) \\
        ...     .set_column_separator('|') \\
        ...     .set_escape_char('\\\\') \\
        ...     .set_use_header() \\
        ...     .set_strict_headers() \\
        ...     .build()
        >>> source = FileSource.for_record_stream_format(
        ...     CsvReaderFormat.for_schema(schema), CSV_FILE_PATH).build()
        >>> ds = env.from_source(source, WatermarkStrategy.no_watermarks(), 'csv-source')
        >>> # the type of records is Types.ROW_NAMED(['id', 'name', 'list'],
        >>> #   [Types.INT(), Types.STRING(), Types.LIST(Types.STRING())])

    .. versionadded:: 1.16.0
    """

    def __init__(self, j_csv_format):
        super().__init__(j_csv_format)

    @staticmethod
    def for_schema(schema: 'CsvSchema') -> 'CsvReaderFormat':
        """
        Builds a :class:`CsvReaderFormat` using `CsvSchema`.
        """
        jvm = get_gateway().jvm
        j_csv_format = jvm.org.apache.flink.formats.csv.CsvReaderFormatFactory \
            .createCsvReaderFormat(
                schema._j_schema,
                _to_java_data_type(schema._data_type)
            )
        return CsvReaderFormat(j_csv_format)
