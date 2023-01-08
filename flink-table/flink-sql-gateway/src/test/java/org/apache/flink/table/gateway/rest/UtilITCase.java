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

package org.apache.flink.table.gateway.rest;

import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.table.gateway.rest.handler.AbstractSqlGatewayRestHandler;
import org.apache.flink.table.gateway.rest.handler.util.GetInfoHandler;
import org.apache.flink.table.gateway.rest.header.util.GetApiVersionHeaders;
import org.apache.flink.table.gateway.rest.header.util.GetInfoHeaders;
import org.apache.flink.table.gateway.rest.message.util.GetApiVersionResponseBody;
import org.apache.flink.table.gateway.rest.message.util.GetInfoResponseBody;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestAPIVersion;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test basic logic of handlers inherited from {@link AbstractSqlGatewayRestHandler} in util related
 * cases.
 */
class UtilITCase extends RestAPIITCaseBase {

    private static final GetInfoHeaders getInfoHeaders = GetInfoHeaders.getInstance();
    private static final EmptyRequestBody emptyRequestBody = EmptyRequestBody.getInstance();
    private static final EmptyMessageParameters emptyParameters =
            EmptyMessageParameters.getInstance();
    private static final GetApiVersionHeaders getApiVersionHeaders =
            GetApiVersionHeaders.getInstance();

    @Test
    void testGetInfoAndApiVersion() throws Exception {
        CompletableFuture<GetInfoResponseBody> response =
                sendRequest(getInfoHeaders, emptyParameters, emptyRequestBody);
        String productName = response.get().getProductName();
        String version = response.get().getProductVersion();
        assertEquals(GetInfoHandler.PRODUCT_NAME, productName);
        assertEquals(EnvironmentInformation.getVersion(), version);

        CompletableFuture<GetApiVersionResponseBody> response2 =
                sendRequest(getApiVersionHeaders, emptyParameters, emptyRequestBody);
        List<String> versions = response2.get().getVersions();
        assertThat(
                        Arrays.stream(SqlGatewayRestAPIVersion.values())
                                .filter(SqlGatewayRestAPIVersion::isStableVersion)
                                .map(Enum::name)
                                .collect(Collectors.toList()))
                .isEqualTo(versions);
    }
}
