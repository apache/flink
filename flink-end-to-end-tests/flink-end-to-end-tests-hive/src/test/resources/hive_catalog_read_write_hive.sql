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
SET 'sql-client.verbose' = 'true';

create catalog hivecatalog with (
 'type' = 'hive',
 'hive-conf-dir' = '$VAR_HIVE_CONF_DIR'
);

use catalog hivecatalog;

create table hive_sink1 (
 a int,
 b varchar(10)
) with ('connector' = 'hive', 'hive.location-uri' = '$VAR_HIVE_WAREHOUSE/hive_sink1');

insert into hive_sink1 values (1, 'v1'), (2, 'v2');

create table hive_sink2(
  a int,
  b varchar(10)
) with ('connector' = 'hive', 'hive.location-uri' = '$VAR_HIVE_WAREHOUSE/hive_sink2');

insert into hive_sink2 select * from hive_sink1;
