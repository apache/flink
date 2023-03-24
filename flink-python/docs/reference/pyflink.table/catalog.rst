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
    CatalogBaseTable.get_schema
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
    CatalogFunction.is_generic
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
