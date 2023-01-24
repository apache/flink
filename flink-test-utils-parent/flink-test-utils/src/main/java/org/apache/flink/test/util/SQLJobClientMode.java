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

package org.apache.flink.test.util;

/** Use which client to submit job. */
public interface SQLJobClientMode {

    static EmbeddedSqlClient getEmbeddedSqlClient() {
        return EmbeddedSqlClient.INSTANCE;
    }

    static GatewaySqlClient getGatewaySqlClient(String host, int port) {
        return new GatewaySqlClient(host, port);
    }

    static HiveJDBC getHiveJDBC(String host, int port) {
        return new HiveJDBC(host, port);
    }

    static RestClient getRestClient(String host, int port, String getRestEndpointVersion) {
        return new RestClient(host, port, getRestEndpointVersion);
    }

    /** Use the Sql Client embedded mode to submit jobs. */
    class EmbeddedSqlClient implements SQLJobClientMode {

        public static final EmbeddedSqlClient INSTANCE = new EmbeddedSqlClient();

        private EmbeddedSqlClient() {}
    }

    /** The base gateway mode to submit jobs. */
    class GatewayClientMode implements SQLJobClientMode {
        private final String host;
        private final int port;

        public GatewayClientMode(String host, int port) {
            this.host = host;
            this.port = port;
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }
    }

    /** Uses the Sql Client gateway mode to submit jobs. */
    class GatewaySqlClient extends GatewayClientMode {

        public GatewaySqlClient(String host, int port) {
            super(host, port);
        }
    }

    /** Uses the Hive JDBC to submit jobs. */
    class HiveJDBC extends GatewayClientMode {

        public HiveJDBC(String host, int port) {
            super(host, port);
        }
    }

    /** Uses a REST Client to submit jobs. */
    class RestClient extends GatewayClientMode {

        private final String restEndpointVersion;

        public RestClient(String host, int port, String restEndpointVersion) {
            super(host, port);
            this.restEndpointVersion = restEndpointVersion;
        }

        public String getRestEndpointVersion() {
            return restEndpointVersion;
        }
    }
}
