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

=======
Catalog
=======

Catalog
-------

Catalog is responsible for reading and writing metadata such as database/table/views/UDFs from a
registered catalog. It connects a registered catalog and Flink's Table API.

.. currentmodule:: pyflink.table.catalog

.. autosummary::
    :toctree: api/

    Catalog.get_default_database
    Catalog.list_databases
    Catalog.get_database
    Catalog.database_exists
    Catalog.create_database
    Catalog.drop_database
    Catalog.alter_database
    Catalog.list_tables
    Catalog.list_views
    Catalog.get_table
    Catalog.table_exists
    Catalog.drop_table
    Catalog.rename_table
    Catalog.create_table
    Catalog.alter_table
    Catalog.list_partitions
    Catalog.get_partition
    Catalog.partition_exists
    Catalog.create_partition
    Catalog.drop_partition
    Catalog.alter_partition
    Catalog.list_functions
    Catalog.get_function
    Catalog.function_exists
    Catalog.create_function
    Catalog.alter_function
    Catalog.drop_function
    Catalog.get_table_statistics
    Catalog.get_table_column_statistics
    Catalog.get_partition_statistics
    Catalog.bulk_get_partition_statistics
    Catalog.get_partition_column_statistics
    Catalog.bulk_get_partition_column_statistics
    Catalog.alter_table_statistics
    Catalog.alter_table_column_statistics
    Catalog.alter_partition_statistics
    Catalog.alter_partition_column_statistics


CatalogDatabase
---------------

Represents a database object in a catalog.

.. currentmodule:: pyflink.table.catalog

.. autosummary::
    :toctree: api/

    CatalogDatabase.create_instance
    CatalogDatabase.get_properties
    CatalogDatabase.get_comment
    CatalogDatabase.copy
    CatalogDatabase.get_description
    CatalogDatabase.get_detailed_description


CatalogBaseTable
----------------

CatalogBaseTable is the common parent of table and view. It has a map of key-value pairs defining
the properties of the table.

.. currentmodule:: pyflink.table.catalog

.. autosummary::
    :toctree: api/

    CatalogBaseTable.create_table
    CatalogBaseTable.create_view
    CatalogBaseTable.get_options
    CatalogBaseTable.get_unresolved_schema
    CatalogBaseTable.get_comment
    CatalogBaseTable.copy
    CatalogBaseTable.get_description
    CatalogBaseTable.get_detailed_description


CatalogPartition
----------------

Represents a partition object in catalog.

.. currentmodule:: pyflink.table.catalog

.. autosummary::
    :toctree: api/

    CatalogPartition.create_instance
    CatalogPartition.get_properties
    CatalogPartition.copy
    CatalogPartition.get_description
    CatalogPartition.get_detailed_description
    CatalogPartition.get_comment


CatalogFunction
---------------

Represents a partition object in catalog.

.. currentmodule:: pyflink.table.catalog

.. autosummary::
    :toctree: api/

    CatalogFunction.create_instance
    CatalogFunction.get_class_name
    CatalogFunction.copy
    CatalogFunction.get_description
    CatalogFunction.get_detailed_description
    CatalogFunction.get_function_language


ObjectPath
----------

A database name and object (table/view/function) name combo in a catalog.

.. currentmodule:: pyflink.table.catalog

.. autosummary::
    :toctree: api/

    ObjectPath.from_string
    ObjectPath.get_database_name
    ObjectPath.get_object_name
    ObjectPath.get_full_name


CatalogPartitionSpec
--------------------

Represents a partition spec object in catalog.
Partition columns and values are NOT of strict order, and they need to be re-arranged to the
correct order by comparing with a list of strictly ordered partition keys.

.. currentmodule:: pyflink.table.catalog

.. autosummary::
    :toctree: api/

    CatalogPartitionSpec.get_partition_spec


CatalogTableStatistics
----------------------

Statistics for a non-partitioned table or a partition of a partitioned table.

.. currentmodule:: pyflink.table.catalog

.. autosummary::
    :toctree: api/

    CatalogTableStatistics.get_row_count
    CatalogTableStatistics.get_field_count
    CatalogTableStatistics.get_total_size
    CatalogTableStatistics.get_raw_data_size
    CatalogTableStatistics.get_properties
    CatalogTableStatistics.copy


CatalogColumnStatistics
-----------------------

Column statistics of a table or partition.

.. currentmodule:: pyflink.table.catalog

.. autosummary::
    :toctree: api/

    CatalogColumnStatistics.get_column_statistics_data
    CatalogColumnStatistics.get_properties
    CatalogColumnStatistics.copy


HiveCatalog
-----------

A catalog implementation for Hive.

.. currentmodule:: pyflink.table.catalog

.. autosummary::
    :toctree: api/

    HiveCatalog



JdbcCatalog
-----------

A catalog implementation for Jdbc.

.. currentmodule:: pyflink.table.catalog

.. autosummary::
    :toctree: api/

    JdbcCatalog



CatalogDescriptor
-----------------

Describes a catalog with the catalog name and configuration.
A CatalogDescriptor is a template for creating a catalog instance. It closely resembles the
"CREATE CATALOG" SQL DDL statement, containing catalog name and catalog configuration.

.. currentmodule:: pyflink.table.catalog

.. autosummary::
    :toctree: api/

    CatalogDescriptor.of



ObjectIdentifier
----------------

Identifies an object in a catalog, including tables, views, function, or types.
An :class:`ObjectIdentifier` must be fully qualified. It is the responsibility of the catalog
manager to resolve an :class:`ObjectIdentifier` to an object.

While Path :class:`ObjectPath` is used within the same catalog, instances of this class can be
used across catalogs. An :class:`ObjectPath` only describes the name and database of an
object and so is scoped over a particular catalog, but an :class:`ObjectIdentifier` is fully
qualified and describes the name, database and catalog of the object.

Two objects are considered equal if they share the same :class:`ObjectIdentifier` in a session
context, such as a :class:`~pyflink.table.TableEnvironment`, where catalogs (or objects in a
catalog) have not been added, deleted or modified.

.. currentmodule:: pyflink.table.catalog

.. autosummary::
    :toctree: api/

    ObjectIdentifier.of
    ObjectIdentifier.get_catalog_name
    ObjectIdentifier.get_database_name
    ObjectIdentifier.get_object_name
    ObjectIdentifier.to_object_path
    ObjectIdentifier.to_list
    ObjectIdentifier.as_serializable_string
    ObjectIdentifier.as_summary_string


Column
------

Representation of a column in a :class:`~pyflink.table.catalog.ResolvedSchema`.

A table column describes either a :class:`pyflink.table.catalog.PhysicalColumn`,
:class:`pyflink.table.catalog.ComputedColumn`, or :class:`pyflink.table.catalog.MetadataColumn`.

.. currentmodule:: pyflink.table.catalog

.. autosummary::
    :toctree: api/

    Column.physical
    Column.computed
    Column.metadata
    Column.with_comment
    Column.is_physical
    Column.is_persisted
    Column.get_data_type
    Column.get_name
    Column.get_comment
    Column.as_summary_string
    Column.explain_extras
    Column.copy
    Column.rename


WatermarkSpec
-------------

Representation of a watermark specification in :class:`~pyflink.table.catalog.ResolvedSchema`.

It defines the rowtime attribute and a :class:`~pyflink.table.ResolvedExpression`
for watermark generation.

.. currentmodule:: pyflink.table.catalog

.. autosummary::
    :toctree: api/

    WatermarkSpec.of
    WatermarkSpec.get_rowtime_attribute
    WatermarkSpec.get_watermark_expression
    WatermarkSpec.as_summary_string


Constraint
----------

Integrity constraints, generally referred to simply as constraints, define the valid states of
SQL-data by constraining the values in the base tables.

.. currentmodule:: pyflink.table.catalog

.. autosummary::
    :toctree: api/

    Constraint.get_name
    Constraint.is_enforced
    Constraint.get_type
    Constraint.as_summary_string

UniqueConstraint
................

A unique key constraint. It can be declared also as a PRIMARY KEY.

.. currentmodule:: pyflink.table.catalog

.. autosummary::
    :toctree: api/

    UniqueConstraint.get_columns
    UniqueConstraint.get_type_string

ResolvedSchema
--------------

Schema of a table or view consisting of columns, constraints, and watermark specifications.

This class is the result of resolving a :class:`~pyflink.table.Schema` into a final validated
representation.

- Data types and functions have been expanded to fully qualified identifiers.
- Time attributes are represented in the column's data type.
- :class:`pyflink.table.Expression` have been translated to
  :class:`pyflink.table.catalog.ResolvedExpression`

This class should not be passed into a connector. It is therefore also not serializable.
Instead, the :func:`~pyflink.table.catalog.ResolvedSchema.to_physical_row_data_type` can be
passed around where necessary.

.. currentmodule:: pyflink.table.catalog

.. autosummary::
    :toctree: api/

    ResolvedSchema.of
    ResolvedSchema.physical
    ResolvedSchema.get_column_count
    ResolvedSchema.get_columns
    ResolvedSchema.get_column_names
    ResolvedSchema.get_column_data_types
    ResolvedSchema.get_column
    ResolvedSchema.get_watermark_specs
    ResolvedSchema.get_primary_key
    ResolvedSchema.get_primary_key_indexes
    ResolvedSchema.to_source_row_data_type
    ResolvedSchema.to_physical_row_data_type
    ResolvedSchema.to_sink_row_data_type
