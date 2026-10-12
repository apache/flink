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

========
Catalogs
========

Functions for creating catalogs and navigating catalogs and databases. Setting the current
catalog and database lets :func:`~pyflink.dataframe.read_catalog_table` and
:meth:`~pyflink.dataframe.DataFrame.write_catalog_table` reference tables by short paths instead
of the full ``catalog_name.db_name.table_name``.

Catalog Management
------------------

.. currentmodule:: pyflink.dataframe

.. autosummary::
    :toctree: api/

    create_catalog
    get_catalog
    use_catalog
    get_current_catalog
    list_catalogs

Database Navigation
-------------------

.. currentmodule:: pyflink.dataframe

.. autosummary::
    :toctree: api/

    use_database
    get_current_database
    list_databases
