# table_sensitive.q - CREATE/SHOW TABLE
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# ==========================================================================
# test create catalog and table with sensitive options
# ==========================================================================

# test create jdbc catalog
CREATE CATALOG my_catalog WITH (
  'type' = 'test-catalog',
  'username' = 'myuser',
  'password' = 'mypassword'
);
!output
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok

# show jdbc catalog with masked password
SHOW CREATE CATALOG my_catalog;
!output
CREATE CATALOG `my_catalog`
WITH (
  'password' = '****',
  'type' = 'test-catalog',
  'username' = 'myuser'
)
!ok

# test create table with sensitive option
CREATE TABLE IF NOT EXISTS users (
 `id` BIGINT NOT NULL,
 name VARCHAR(32)
) with (
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://localhost:3306/mydatabase',
   'table-name' = 'users',
   'username' = 'my_user',
   'password' = 'my_pass'
);
!output
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok

# test SHOW CREATE TABLE
SHOW CREATE TABLE users;
!output
CREATE TABLE `default_catalog`.`default_database`.`users` (
  `id` BIGINT NOT NULL,
  `name` VARCHAR(32)
)
WITH (
  'connector' = 'jdbc',
  'password' = '****',
  'table-name' = 'users',
  'url' = 'jdbc:mysql://localhost:3306/mydatabase',
  'username' = 'my_user'
)
!ok
