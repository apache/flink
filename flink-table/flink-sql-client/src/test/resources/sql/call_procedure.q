# call-procedure.q - CALL
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

create catalog test_procedure_catalog with ('type'='test_procedure_catalog');
[INFO] Execute statement succeed.
!info

call `test_procedure_catalog`.`system`.generate_n(2);
+--------+
| result |
+--------+
|      0 |
|      1 |
+--------+
2 rows in set
!ok

call `test_procedure_catalog`.`system`.generate_n(1);
+--------+
| result |
+--------+
|      0 |
+--------+
1 row in set
!ok

# switch current catalog to test_procedure_catalog
use catalog test_procedure_catalog;
[INFO] Execute statement succeed.
!info

# create a database `system` to avoid DatabaseNotExistException in the following `show procedure` statement
create database `system`;
[INFO] Execute statement succeed.
!info

show procedures in `system` ilike 'gEnerate%';
+----------------+
| procedure name |
+----------------+
|     generate_n |
|  generate_user |
+----------------+
2 rows in set
!ok

# test call procedure will pojo as return type
call `system`.generate_user('yuxia', 18);
+-------+-----+
|  name | age |
+-------+-----+
| yuxia |  18 |
+-------+-----+
1 row in set
!ok
