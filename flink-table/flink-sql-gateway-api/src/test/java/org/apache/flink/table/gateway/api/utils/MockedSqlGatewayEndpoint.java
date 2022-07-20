/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.table.gateway.api.utils;

import org.apache.flink.table.gateway.api.endpoint.SqlGatewayEndpoint;

import java.util.Objects;

/** Mocked {@link SqlGatewayEndpoint}. */
public class MockedSqlGatewayEndpoint implements SqlGatewayEndpoint {

    private final String host;
    private final int port;
    private final String description;

    public MockedSqlGatewayEndpoint(String host, int port, String description) {
        this.host = host;
        this.port = port;
        this.description = description;
    }

    @Override
    public void start() throws Exception {
        // do nothing
    }

    @Override
    public void stop() throws Exception {
        // do nothing
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MockedSqlGatewayEndpoint)) {
            return false;
        }
        MockedSqlGatewayEndpoint that = (MockedSqlGatewayEndpoint) o;
        return port == that.port
                && Objects.equals(host, that.host)
                && Objects.equals(description, that.description);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port, description);
    }
}
