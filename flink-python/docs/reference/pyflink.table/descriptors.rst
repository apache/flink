.. ################################################################################
     Licensed to the Apache Software Foundation (ASF) under one
     or more contributor license agreements.  See the NOTICE file
     distributed with this work for additional information
     regarding copyright ownership.  The ASF licenses this file
     to you under the Apache License, Version 2.0 (the
     "License"); you may not use this file except in compliance
     with the License.  You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

     Unless required by applicable law or agreed to in writing, software
     distributed under the License is distributed on an "AS IS" BASIS,
     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     See the License for the specific language governing permissions and
    limitations under the License.
   ################################################################################


===========
Descriptors
===========

TableDescriptor
---------------

Describes a CatalogTable representing a source or sink.

TableDescriptor is a template for creating a CatalogTable instance. It closely resembles the
"CREATE TABLE" SQL DDL statement, containing schema, connector options, and other
characteristics. Since tables in Flink are typically backed by external systems, the
descriptor describes how a connector (and possibly its format) are configured.

This can be used to register a table in the Table API, see :func:`create_temporary_table` in
TableEnvironment.

.. currentmodule:: pyflink.table.table_descriptor

.. autosummary::
    :toctree: api/

    TableDescriptor.for_connector
    TableDescriptor.get_schema
    TableDescriptor.get_options
    TableDescriptor.get_partition_keys
    TableDescriptor.get_comment
    TableDescriptor.Builder.schema
    TableDescriptor.Builder.option
    TableDescriptor.Builder.format
    TableDescriptor.Builder.partitioned_by
    TableDescriptor.Builder.comment
    TableDescriptor.Builder.build

FormatDescriptor
----------------

Describes a Format and its options for use with :class:`~pyflink.table.TableDescriptor`.

Formats are responsible for encoding and decoding data in table connectors. Note that not
every connector has a format, while others may have multiple formats (e.g. the Kafka connector
has separate formats for keys and values). Common formats are "json", "csv", "avro", etc.

.. currentmodule:: pyflink.table.table_descriptor

.. autosummary::
    :toctree: api/

    FormatDescriptor.for_format
    FormatDescriptor.get_format
    FormatDescriptor.get_options
    FormatDescriptor.Builder.option
    FormatDescriptor.Builder.build

Schema
------

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

.. currentmodule:: pyflink.table.schema

.. autosummary::
    :toctree: api/

    Schema.Builder.from_schema
    Schema.Builder.from_row_data_type
    Schema.Builder.from_fields
    Schema.Builder.column
    Schema.Builder.column_by_expression
    Schema.Builder.column_by_metadata
    Schema.Builder.watermark
    Schema.Builder.primary_key
    Schema.Builder.primary_key_named
    Schema.Builder.build


TableSchema
-----------

A table schema that represents a table's structure with field names and data types.

.. currentmodule:: pyflink.table.table_schema

.. autosummary::
    :toctree: api/

    TableSchema.copy
    TableSchema.get_field_data_types
    TableSchema.get_field_data_type
    TableSchema.get_field_count
    TableSchema.get_field_names
    TableSchema.get_field_name
    TableSchema.to_row_data_type
    TableSchema.Builder.field
    TableSchema.Builder.build


ChangelogMode
-------------

The set of changes contained in a changelog.

.. currentmodule:: pyflink.table.changelog_mode

.. autosummary::
    :toctree: api/

    ChangelogMode.insert_only
    ChangelogMode.upsert
    ChangelogMode.all
