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

package org.apache.flink.table.gateway.service.utils;

import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.gateway.api.SqlGatewayService;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.api.utils.MockedEndpointVersion;
import org.apache.flink.table.gateway.service.SqlGatewayServiceITCase;
import org.apache.flink.table.gateway.service.SqlGatewayServiceImpl;
import org.apache.flink.table.gateway.service.SqlGatewayServiceStatementITCase;

/** Test util for {@link SqlGatewayServiceITCase} and {@link SqlGatewayServiceStatementITCase}. */
public class SqlGatewayServiceTestUtil {

    public static SessionHandle createInitializedSession(SqlGatewayService service) {
        SessionEnvironment environment =
                SessionEnvironment.newBuilder()
                        .setSessionEndpointVersion(MockedEndpointVersion.V1)
                        .registerCatalog("cat1", new GenericInMemoryCatalog("cat1"))
                        .registerCatalog("cat2", new GenericInMemoryCatalog("cat2"))
                        .build();
        SessionHandle sessionHandle = service.openSession(environment);

        // catalogs: cat1 | cat2
        //     cat1: db1 | db2
        //         db1: temporary table tbl1, table tbl2, temporary view tbl3, view tbl4
        //         db2: table tbl1, view tbl2
        //     cat2 db0
        //         db0: table tbl0
        TableEnvironmentInternal tableEnv =
                ((SqlGatewayServiceImpl) service)
                        .getSession(sessionHandle)
                        .createExecutor()
                        .getTableEnvironment();
        tableEnv.executeSql("CREATE DATABASE cat1.db1");
        tableEnv.executeSql("CREATE TEMPORARY TABLE cat1.db1.tbl1 WITH ('connector' = 'values')");
        tableEnv.executeSql("CREATE TABLE cat1.db1.tbl2 WITH('connector' = 'values')");
        tableEnv.executeSql("CREATE TEMPORARY VIEW cat1.db1.tbl3 AS SELECT 1");
        tableEnv.executeSql("CREATE VIEW cat1.db1.tbl4 AS SELECT 1");

        tableEnv.executeSql("CREATE DATABASE cat1.db2");
        tableEnv.executeSql("CREATE TABLE cat1.db2.tbl1 WITH ('connector' = 'values')");
        tableEnv.executeSql("CREATE VIEW cat1.db2.tbl2 AS SELECT 1");

        tableEnv.executeSql("CREATE DATABASE cat2.db0");
        tableEnv.executeSql("CREATE TABLE cat2.db0.tbl0 WITH('connector' = 'values')");

        return sessionHandle;
    }

    public static ResultSet fetchResults(
            SqlGatewayService service,
            SessionHandle sessionHandle,
            OperationHandle operationHandle) {
        return service.fetchResults(sessionHandle, operationHandle, 0, Integer.MAX_VALUE);
    }

    public static void awaitOperationTermination(
            SqlGatewayService service, SessionHandle sessionHandle, OperationHandle operationHandle)
            throws Exception {
        ((SqlGatewayServiceImpl) service)
                .getSession(sessionHandle)
                .getOperationManager()
                .awaitOperationTermination(operationHandle);
    }
}
