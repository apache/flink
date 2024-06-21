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

package org.apache.flink.table.gateway.rest.header.materializedtable;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.versioning.RestAPIVersion;
import org.apache.flink.table.gateway.rest.header.SqlGatewayMessageHeaders;
import org.apache.flink.table.gateway.rest.message.materializedtable.MaterializedTableIdentifierPathParameter;
import org.apache.flink.table.gateway.rest.message.materializedtable.RefreshMaterializedTableParameters;
import org.apache.flink.table.gateway.rest.message.materializedtable.RefreshMaterializedTableRequestBody;
import org.apache.flink.table.gateway.rest.message.materializedtable.RefreshMaterializedTableResponseBody;
import org.apache.flink.table.gateway.rest.message.session.SessionHandleIdPathParameter;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestAPIVersion;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import java.util.Collection;
import java.util.Collections;

/** Message headers for executing a materialized table refresh operation. */
public class RefreshMaterializedTableHeaders
        implements SqlGatewayMessageHeaders<
                RefreshMaterializedTableRequestBody,
                RefreshMaterializedTableResponseBody,
                RefreshMaterializedTableParameters> {

    private static final RefreshMaterializedTableHeaders INSTANCE =
            new RefreshMaterializedTableHeaders();

    private static final String URL =
            "/sessions/:"
                    + SessionHandleIdPathParameter.KEY
                    + "/materialized-tables/:"
                    + MaterializedTableIdentifierPathParameter.KEY
                    + "/refresh";

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.POST;
    }

    @Override
    public String getTargetRestEndpointURL() {
        return URL;
    }

    @Override
    public Class<RefreshMaterializedTableResponseBody> getResponseClass() {
        return RefreshMaterializedTableResponseBody.class;
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.OK;
    }

    @Override
    public String getDescription() {
        return "Refresh materialized table";
    }

    @Override
    public Class<RefreshMaterializedTableRequestBody> getRequestClass() {
        return RefreshMaterializedTableRequestBody.class;
    }

    @Override
    public RefreshMaterializedTableParameters getUnresolvedMessageParameters() {
        return new RefreshMaterializedTableParameters();
    }

    @Override
    public Collection<? extends RestAPIVersion<?>> getSupportedAPIVersions() {
        return Collections.singleton(SqlGatewayRestAPIVersion.V3);
    }

    public static RefreshMaterializedTableHeaders getInstance() {
        return INSTANCE;
    }

    @Override
    public String operationId() {
        return "refreshMaterializedTable";
    }
}
