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

package org.apache.flink.table.endpoint.hive.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.endpoint.hive.HiveServer2Endpoint;
import org.apache.flink.table.gateway.api.SqlGatewayService;
import org.apache.flink.util.NetUtils;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.function.Supplier;

import static org.apache.flink.table.endpoint.hive.HiveServer2EndpointConfigOptions.CATALOG_DEFAULT_DATABASE;
import static org.apache.flink.table.endpoint.hive.HiveServer2EndpointConfigOptions.CATALOG_NAME;
import static org.apache.flink.table.endpoint.hive.HiveServer2EndpointConfigOptions.MODULE_NAME;
import static org.apache.flink.table.endpoint.hive.HiveServer2EndpointConfigOptions.THRIFT_LOGIN_BEBACKOFF_SLOT_LENGTH;
import static org.apache.flink.table.endpoint.hive.HiveServer2EndpointConfigOptions.THRIFT_LOGIN_TIMEOUT;
import static org.apache.flink.table.endpoint.hive.HiveServer2EndpointConfigOptions.THRIFT_MAX_MESSAGE_SIZE;
import static org.apache.flink.table.endpoint.hive.HiveServer2EndpointConfigOptions.THRIFT_WORKER_KEEPALIVE_TIME;
import static org.apache.flink.table.endpoint.hive.HiveServer2EndpointConfigOptions.THRIFT_WORKER_THREADS_MAX;
import static org.apache.flink.table.endpoint.hive.HiveServer2EndpointConfigOptions.THRIFT_WORKER_THREADS_MIN;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Extension for HiveServer2 endpoint. */
public class HiveServer2EndpointExtension implements BeforeAllCallback, AfterAllCallback {

    private final Supplier<SqlGatewayService> serviceSupplier;
    private final Configuration endpointConfig;

    private NetUtils.Port port;
    private HiveServer2Endpoint endpoint;

    public HiveServer2EndpointExtension(Supplier<SqlGatewayService> serviceSupplier) {
        this(serviceSupplier, new Configuration());
    }

    public HiveServer2EndpointExtension(
            Supplier<SqlGatewayService> serviceSupplier, Configuration configuration) {
        this.serviceSupplier = serviceSupplier;
        this.endpointConfig = configuration;
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        port = NetUtils.getAvailablePort();

        endpoint =
                new HiveServer2Endpoint(
                        serviceSupplier.get(),
                        InetAddress.getLocalHost(),
                        port.getPort(),
                        checkNotNull(endpointConfig.get(THRIFT_MAX_MESSAGE_SIZE)),
                        (int) endpointConfig.get(THRIFT_LOGIN_TIMEOUT).toMillis(),
                        (int) endpointConfig.get(THRIFT_LOGIN_BEBACKOFF_SLOT_LENGTH).toMillis(),
                        endpointConfig.get(THRIFT_WORKER_THREADS_MIN),
                        endpointConfig.get(THRIFT_WORKER_THREADS_MAX),
                        endpointConfig.get(THRIFT_WORKER_KEEPALIVE_TIME),
                        endpointConfig.get(CATALOG_NAME),
                        HiveTestUtils.createHiveSite().getParent(),
                        endpointConfig.get(CATALOG_DEFAULT_DATABASE),
                        endpointConfig.get(MODULE_NAME),
                        true,
                        false);
        endpoint.start();
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        if (port != null) {
            port.close();
        }
        if (endpoint != null) {
            endpoint.stop();
        }
    }

    public int getPort() {
        return checkNotNull(port).getPort();
    }

    public HiveServer2Endpoint getEndpoint() {
        return endpoint;
    }

    public Connection getConnection() throws Exception {
        // In hive3, if "hive.metastore.schema.verification" is true, the
        // "datanucleus.schema.autoCreateTables" is false during the creation of the HiveConf.
        // So we need to manually enable datanucleus.schema.autoCreateAll
        // Please cc FLINK-27999 for more details
        return DriverManager.getConnection(
                String.format(
                        "jdbc:hive2://%s:%s/default;auth=noSasl?datanucleus.schema.autoCreateAll=true",
                        InetAddress.getLocalHost().getHostAddress(), getPort()));
    }
}
