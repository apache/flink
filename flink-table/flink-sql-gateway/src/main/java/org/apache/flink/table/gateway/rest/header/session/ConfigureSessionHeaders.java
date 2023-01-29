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

package org.apache.flink.table.gateway.rest.header.session;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.versioning.RestAPIVersion;
import org.apache.flink.table.gateway.rest.header.SqlGatewayMessageHeaders;
import org.apache.flink.table.gateway.rest.message.session.ConfigureSessionRequestBody;
import org.apache.flink.table.gateway.rest.message.session.SessionHandleIdPathParameter;
import org.apache.flink.table.gateway.rest.message.session.SessionMessageParameters;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestAPIVersion;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import java.util.Collection;
import java.util.Collections;

/** Message headers for configuring a session. */
public class ConfigureSessionHeaders
        implements SqlGatewayMessageHeaders<
                ConfigureSessionRequestBody, EmptyResponseBody, SessionMessageParameters> {

    private static final ConfigureSessionHeaders INSTANCE = new ConfigureSessionHeaders();

    private static final String URL =
            "/sessions/:" + SessionHandleIdPathParameter.KEY + "/configure-session";

    @Override
    public Class<EmptyResponseBody> getResponseClass() {
        return EmptyResponseBody.class;
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.OK;
    }

    @Override
    public String getDescription() {
        return "Configures the session with the statement which could be:\n"
                + "CREATE TABLE, DROP TABLE, ALTER TABLE, "
                + "CREATE DATABASE, DROP DATABASE, ALTER DATABASE, "
                + "CREATE FUNCTION, DROP FUNCTION, ALTER FUNCTION, "
                + "CREATE CATALOG, DROP CATALOG, "
                + "USE CATALOG, USE [CATALOG.]DATABASE, "
                + "CREATE VIEW, DROP VIEW, "
                + "LOAD MODULE, UNLOAD MODULE, USE MODULE, "
                + "ADD JAR.";
    }

    @Override
    public Class<ConfigureSessionRequestBody> getRequestClass() {
        return ConfigureSessionRequestBody.class;
    }

    @Override
    public SessionMessageParameters getUnresolvedMessageParameters() {
        return new SessionMessageParameters();
    }

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.POST;
    }

    @Override
    public String getTargetRestEndpointURL() {
        return URL;
    }

    @Override
    public Collection<? extends RestAPIVersion<?>> getSupportedAPIVersions() {
        return Collections.singleton(SqlGatewayRestAPIVersion.V2);
    }

    public static ConfigureSessionHeaders getInstance() {
        return INSTANCE;
    }

    @Override
    public String operationId() {
        return "configureSession";
    }
}
