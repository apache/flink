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

package org.apache.flink.table.gateway.service;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.gateway.api.SqlGatewayService;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.refresh.ContinuousRefreshHandler;
import org.apache.flink.table.refresh.ContinuousRefreshHandlerSerializer;

import java.io.IOException;

/** Helpers shared between the materialized table gateway ITCases. */
final class MaterializedTableTestUtils {

    private MaterializedTableTestUtils() {}

    static OperationHandle executeStatement(
            SqlGatewayService service, SessionHandle sessionHandle, String statement) {
        return service.executeStatement(sessionHandle, statement, -1, new Configuration());
    }

    static ResolvedCatalogMaterializedTable getTable(
            SqlGatewayService service, SessionHandle sessionHandle, ObjectIdentifier identifier) {
        return (ResolvedCatalogMaterializedTable) service.getTable(sessionHandle, identifier);
    }

    static ContinuousRefreshHandler getContinuousRefreshHandler(
            ResolvedCatalogMaterializedTable resolvedTable, ClassLoader classLoader)
            throws IOException, ClassNotFoundException {
        return ContinuousRefreshHandlerSerializer.INSTANCE.deserialize(
                resolvedTable.getSerializedRefreshHandler(), classLoader);
    }
}
