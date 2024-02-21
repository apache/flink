/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';
set 'sql-client.verbose' = 'true';

create catalog hivecatalog with (
 'type' = 'hive',
 'hive-conf-dir' = '$VAR_HIVE_CONF_DIR'
);

use catalog hivecatalog;

-- switch to hive dialect
set 'table.sql-dialect' = 'hive';

ADD JAR $VAR_UDF_JAR_PATH;

-- create a table sink
create table h_table_sink1(
    a int,
    b string
) LOCATION '$VAR_HIVE_WAREHOUSE/h_table_sink1';

-- prepare data
insert into h_table_sink1 values (1, 'v1'), (2, 'v2'), (3, 'v3');

-- create another table sink
create table  h_table_sink2(
    a int,
    b string
) LOCATION '$VAR_HIVE_WAREHOUSE/h_table_sink2';

-- create function using jar
CREATE FUNCTION hive_add_one as 'HiveAddOneFunc';

-- insert the table with the adding jar
insert into h_table_sink2 select hive_add_one(a), b from h_table_sink1;
