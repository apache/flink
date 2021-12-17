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

from pyflink.java_gateway import get_gateway
from pyflink.table.types import _to_java_type

__all__ = ['TableSource', 'CsvTableSource']


class TableSource(object):
    """
    Defines a table from an external system or location.
    """

    def __init__(self, j_table_source):
        self._j_table_source = j_table_source


class CsvTableSource(TableSource):
    """
    A :class:`TableSource` for simple CSV files with a
    (logically) unlimited number of fields.

    Example:
    ::

        >>> CsvTableSource("/csv/file/path", ["a", "b"], [DataTypes.INT(), DataTypes.STRING()])

    :param source_path: The path to the CSV file.
    :type source_path: str
    :param field_names: The names of the table fields.
    :type field_names: collections.Iterable[str]
    :param field_types: The types of the table fields.
    :type field_types: collections.Iterable[str]
    :param field_delim: The field delimiter, "," by default.
    :type field_delim: str, optional
    :param line_delim: The row delimiter, "\\n" by default.
    :type line_delim: str, optional
    :param quote_character: An optional quote character for String values, null by default.
    :type quote_character: str, optional
    :param ignore_first_line: Flag to ignore the first line, false by default.
    :type ignore_first_line: bool, optional
    :param ignore_comments: An optional prefix to indicate comments, null by default.
    :type ignore_comments: str, optional
    :param lenient: Flag to skip records with parse error instead to fail, false by default.
    :type lenient: bool, optional
    :param empty_column_as_null: Treat empty column as null, false by default.
    :type empty_column_as_null: bool, optional
    """

    def __init__(
        self,
        source_path,
        field_names,
        field_types,
        field_delim=None,
        line_delim=None,
        quote_character=None,
        ignore_first_line=None,
        ignore_comments=None,
        lenient=None,
        empty_column_as_null=None,
    ):
        gateway = get_gateway()

        builder = gateway.jvm.CsvTableSource.builder()
        builder.path(source_path)

        for (field_name, field_type) in zip(field_names, field_types):
            builder.field(field_name, _to_java_type(field_type))

        if field_delim is not None:
            builder.fieldDelimiter(field_delim)

        if line_delim is not None:
            builder.lineDelimiter(line_delim)

        if quote_character is not None:
            # Java API has a Character type for this field. At time of writing,
            # Py4J will convert the Python str to Java Character by taking only
            # the first character.  This results in either:
            #   - Silently truncating a Python str with more than one character
            #     with no further type error from either Py4J or Java
            #     CsvTableSource
            #   - java.lang.StringIndexOutOfBoundsException from Py4J for an
            #     empty Python str.  That error can be made more friendly here.
            if len(quote_character) != 1:
                raise ValueError(
                    "Expected a single CSV quote character but got '{}'".format(quote_character)
                )
            builder.quoteCharacter(quote_character)

        if ignore_first_line:
            builder.ignoreFirstLine()

        if ignore_comments is not None:
            builder.commentPrefix(ignore_comments)

        if lenient:
            builder.ignoreParseErrors()

        if empty_column_as_null:
            builder.emptyColumnAsNull()

        super(CsvTableSource, self).__init__(builder.build())
