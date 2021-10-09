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
from typing import Dict, Union, List, Optional

from pyflink.common.config_options import ConfigOption
from pyflink.java_gateway import get_gateway
from pyflink.table.schema import Schema
from pyflink.util.java_utils import to_jarray

__all__ = ['TableDescriptor', 'FormatDescriptor']


class TableDescriptor(object):
    """
    Describes a CatalogTable representing a source or sink.

    TableDescriptor is a template for creating a CatalogTable instance. It closely resembles the
    "CREATE TABLE" SQL DDL statement, containing schema, connector options, and other
    characteristics. Since tables in Flink are typically backed by external systems, the
    descriptor describes how a connector (and possibly its format) are configured.

    This can be used to register a table in the Table API, see :func:`create_temporary_table` in
    TableEnvironment.
    """

    def __init__(self, j_table_descriptor):
        self._j_table_descriptor = j_table_descriptor

    @staticmethod
    def for_connector(connector: str) -> 'TableDescriptor.Builder':
        """
        Creates a new :class:`~pyflink.table.TableDescriptor.Builder` for a table using the given
        connector.

        :param connector: The factory identifier for the connector.
        """
        gateway = get_gateway()
        j_builder = gateway.jvm.TableDescriptor.forConnector(connector)
        return TableDescriptor.Builder(j_builder)

    def get_schema(self) -> Optional[Schema]:
        j_schema = self._j_table_descriptor.getSchema()
        if j_schema.isPresent():
            return Schema(j_schema.get())
        else:
            return None

    def get_options(self) -> Dict[str, str]:
        return self._j_table_descriptor.getOptions()

    def get_partition_keys(self) -> List[str]:
        return self._j_table_descriptor.getPartitionKeys()

    def get_comment(self) -> Optional[str]:
        j_comment = self._j_table_descriptor.getComment()
        if j_comment.isPresent():
            return j_comment.get()
        else:
            return None

    def __str__(self):
        return self._j_table_descriptor.toString()

    def __eq__(self, other):
        return (self.__class__ == other.__class__ and
                self._j_table_descriptor.equals(other._j_table_descriptor))

    def __hash__(self):
        return self._j_table_descriptor.hashCode()

    class Builder(object):
        """
        Builder for TableDescriptor.
        """

        def __init__(self, j_builder):
            self._j_builder = j_builder

        def schema(self, schema: Schema) -> 'TableDescriptor.Builder':
            """
            Define the schema of the TableDescriptor.
            """
            self._j_builder.schema(schema._j_schema)
            return self

        def option(self, key: Union[str, ConfigOption], value) -> 'TableDescriptor.Builder':
            """
            Sets the given option on the table.

            Option keys must be fully specified. When defining options for a Format, use
            format(FormatDescriptor) instead.

            Example:
            ::

                >>> TableDescriptor.for_connector("kafka") \
                ...     .option("scan.startup.mode", "latest-offset") \
                ...     .build()

            """
            if isinstance(key, str):
                self._j_builder.option(key, value)
            else:
                self._j_builder.option(key._j_config_option, value)
            return self

        def format(self,
                   format: Union[str, 'FormatDescriptor'],
                   format_option: ConfigOption[str] = None) -> 'TableDescriptor.Builder':
            """
            Defines the format to be used for this table.

            Note that not every connector requires a format to be specified, while others may use
            multiple formats.

            Example:
            ::

                >>> TableDescriptor.for_connector("kafka") \
                ...     .format(FormatDescriptor.for_format("json")
                ...                 .option("ignore-parse-errors", "true")
                ...                 .build())

                will result in the options:

                    'format' = 'json'
                    'json.ignore-parse-errors' = 'true'

            """
            if format_option is None:
                if isinstance(format, str):
                    self._j_builder.format(format)
                else:
                    self._j_builder.format(format._j_format_descriptor)
            else:
                if isinstance(format, str):
                    self._j_builder.format(format_option._j_config_option, format)
                else:
                    self._j_builder.format(
                        format_option._j_config_option, format._j_format_descriptor)
            return self

        def partitioned_by(self, *partition_keys: str) -> 'TableDescriptor.Builder':
            """
            Define which columns this table is partitioned by.
            """
            gateway = get_gateway()
            self._j_builder.partitionedBy(to_jarray(gateway.jvm.java.lang.String, partition_keys))
            return self

        def comment(self, comment: str) -> 'TableDescriptor.Builder':
            """
            Define the comment for this table.
            """
            self._j_builder.comment(comment)
            return self

        def build(self) -> 'TableDescriptor':
            """
            Returns an immutable instance of :class:`~pyflink.table.TableDescriptor`.
            """
            return TableDescriptor(self._j_builder.build())


class FormatDescriptor(object):
    """
    Describes a Format and its options for use with :class:`~pyflink.table.TableDescriptor`.

    Formats are responsible for encoding and decoding data in table connectors. Note that not
    every connector has a format, while others may have multiple formats (e.g. the Kafka connector
    has separate formats for keys and values). Common formats are "json", "csv", "avro", etc.
    """

    def __init__(self, j_format_descriptor):
        self._j_format_descriptor = j_format_descriptor

    @staticmethod
    def for_format(format: str) -> 'FormatDescriptor.Builder':
        """
        Creates a new :class:`~pyflink.table.FormatDescriptor.Builder` describing a format with the
        given format identifier.

        :param format: The factory identifier for the format.
        """
        gateway = get_gateway()
        j_builder = gateway.jvm.FormatDescriptor.forFormat(format)
        return FormatDescriptor.Builder(j_builder)

    def get_format(self) -> str:
        return self._j_format_descriptor.getFormat()

    def get_options(self) -> Dict[str, str]:
        return self._j_format_descriptor.getOptions()

    def __str__(self):
        return self._j_format_descriptor.toString()

    def __eq__(self, other):
        return (self.__class__ == other.__class__ and
                self._j_format_descriptor.equals(other._j_format_descriptor))

    def __hash__(self):
        return self._j_format_descriptor.hashCode()

    class Builder(object):
        """
        Builder for FormatDescriptor.
        """

        def __init__(self, j_builder):
            self._j_builder = j_builder

        def option(self, key: Union[str, ConfigOption], value) -> 'FormatDescriptor.Builder':
            """
            Sets the given option on the format.

            Note that format options must not be prefixed with the format identifier itself here.

            Example:
            ::

                >>> FormatDescriptor.for_format("json") \
                ...     .option("ignore-parse-errors", "true") \
                ...     .build()

                will automatically be converted into its prefixed form:

                    'format' = 'json'
                    'json.ignore-parse-errors' = 'true'

            """
            if isinstance(key, str):
                self._j_builder.option(key, value)
            else:
                self._j_builder.option(key._j_config_option, value)
            return self

        def build(self) -> 'FormatDescriptor':
            """
            Returns an immutable instance of :class:`~pyflink.table.FormatDescriptor`.
            """
            return FormatDescriptor(self._j_builder.build())
