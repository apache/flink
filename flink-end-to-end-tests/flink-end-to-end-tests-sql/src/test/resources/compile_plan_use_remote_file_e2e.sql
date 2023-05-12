/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
SET execution.runtime-mode = $MODE;

CREATE TABLE MyTable (
    a bigint,
    b int,
    c varchar
) with (
    'connector' = 'values',
    'bounded' = 'false'
);

CREATE TABLE MySink (
    a bigint,
    b int,
    c varchar
) with (
    'connector' = 'values',
    'table-sink-class' = 'DEFAULT'
);

COMPILE PLAN '$HDFS_Json_Plan_PATH' FOR INSERT INTO MySink SELECT * FROM MyTable;

EXECUTE PLAN '$HDFS_Json_Plan_PATH';

COMPILE and EXECUTE plan '$HDFS_Json_Plan_PATH' FOR INSERT INTO MySink SELECT * FROM MyTable;


