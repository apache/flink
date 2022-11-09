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

==========
Data Types
==========

Describes the data type of a value in the table ecosystem. Instances of this class can be used
to declare input and/or output types of operations.

:class:`DataType` has two responsibilities: declaring a logical type and giving hints
about the physical representation of data to the optimizer. While the logical type is mandatory,
hints are optional but useful at the edges to other APIs.

The logical type is independent of any physical representation and is close to the "data type"
terminology of the SQL standard.

Physical hints are required at the edges of the table ecosystem. Hints indicate the data format
that an implementation expects.

.. currentmodule:: pyflink.table.types

.. autosummary::
    :toctree: api/

    DataTypes.NULL
    DataTypes.CHAR
    DataTypes.VARCHAR
    DataTypes.STRING
    DataTypes.BOOLEAN
    DataTypes.BINARY
    DataTypes.VARBINARY
    DataTypes.BYTES
    DataTypes.DECIMAL
    DataTypes.TINYINT
    DataTypes.SMALLINT
    DataTypes.INT
    DataTypes.BIGINT
    DataTypes.FLOAT
    DataTypes.DOUBLE
    DataTypes.DATE
    DataTypes.TIME
    DataTypes.TIMESTAMP
    DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE
    DataTypes.TIMESTAMP_LTZ
    DataTypes.ARRAY
    DataTypes.LIST_VIEW
    DataTypes.MAP
    DataTypes.MAP_VIEW
    DataTypes.MULTISET
    DataTypes.ROW
    DataTypes.FIELD
    DataTypes.SECOND
    DataTypes.MINUTE
    DataTypes.HOUR
    DataTypes.DAY
    DataTypes.MONTH
    DataTypes.YEAR
    DataTypes.INTERVAL
