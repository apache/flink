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

package org.apache.flink.table.client.cli.utils;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.delegation.Parser;

/** A utility class that provides pre-prepared tables and sql parser. */
public class SqlParserHelper {
    // return the sql parser instance hold by this table evn.
    private final TableEnvironment tableEnv;

    public SqlParserHelper() {
        tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().build());
    }

    /** prepare some tables for testing. */
    public void registerTables() {
        registerTable(
                "create table MyTable (a int, b bigint, c varchar(32)) "
                        + "with ('connector' = 'filesystem', 'path' = '/non', 'format' = 'csv')");
        registerTable(
                "create table MyOtherTable (a int, b bigint) "
                        + "with ('connector' = 'filesystem', 'path' = '/non', 'format' = 'csv')");
        registerTable(
                "create table MySink (a int, c varchar(32)) with ('connector' = 'COLLECTION' )");
        registerTable("create view MyView as select * from MyTable");
    }

    public void registerTable(String createTableStmt) {
        tableEnv.executeSql(createTableStmt);
    }

    public Parser getSqlParser() {
        return ((TableEnvironmentInternal) tableEnv).getParser();
    }
}
