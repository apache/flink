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

package org.apache.flink.table.client;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestEndpointExtension;
import org.apache.flink.table.gateway.service.utils.SqlGatewayServiceExtension;

import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Test that {@link SqlClient} works normally when SSL is enabled. */
class SqlClientSSLTest extends SqlClientTestBase {
    @RegisterExtension
    @Order(1)
    public static final SqlGatewayServiceExtension SQL_GATEWAY_SERVICE_EXTENSION =
            new SqlGatewayServiceExtension(Configuration::new);

    @RegisterExtension
    @Order(2)
    private static final SqlGatewayRestEndpointExtension SQL_GATEWAY_REST_ENDPOINT_EXTENSION =
            new SqlGatewayRestEndpointExtension(
                    SQL_GATEWAY_SERVICE_EXTENSION::getService, SqlClientSSLTest::withSSL);

    private static final String truststorePath = getTestResource("ssl/local127.truststore");

    private static final String keystorePath = getTestResource("ssl/local127.keystore");

    @Test
    void testEmbeddedMode() throws Exception {
        String[] args = new String[] {"embedded"};
        String actual = runSqlClient(args, String.join("\n", "SET;", "QUIT;"), false);
        assertThat(actual).contains(SecurityOptions.SSL_REST_ENABLED.key(), "true");
    }

    @Test
    void testGatewayMode() throws Exception {
        String[] args =
                new String[] {
                    "gateway",
                    "-e",
                    String.format(
                            "%s:%d",
                            SQL_GATEWAY_REST_ENDPOINT_EXTENSION.getTargetAddress(),
                            SQL_GATEWAY_REST_ENDPOINT_EXTENSION.getTargetPort())
                };
        String actual = runSqlClient(args, String.join("\n", "SET;", "QUIT;"), false);
        assertThat(actual).contains(SecurityOptions.SSL_REST_ENABLED.key(), "true");
    }

    private static void withSSL(Configuration configuration) {
        configuration.set(SecurityOptions.SSL_REST_ENABLED, true);
        configuration.set(SecurityOptions.SSL_REST_TRUSTSTORE, truststorePath);
        configuration.set(SecurityOptions.SSL_REST_TRUSTSTORE_PASSWORD, "password");
        configuration.set(SecurityOptions.SSL_REST_KEYSTORE, keystorePath);
        configuration.set(SecurityOptions.SSL_REST_KEYSTORE_PASSWORD, "password");
        configuration.set(SecurityOptions.SSL_REST_KEY_PASSWORD, "password");
    }

    @Override
    protected void writeConfigOptionsToConfYaml(Path confYamlPath) throws IOException {
        Configuration configuration = new Configuration();
        withSSL(configuration);
        Files.write(
                confYamlPath,
                configuration.toMap().entrySet().stream()
                        .map(entry -> entry.getKey() + ": " + entry.getValue())
                        .collect(Collectors.toList()));
    }

    private static String getTestResource(final String fileName) {
        final ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        final URL resource = classLoader.getResource(fileName);
        if (resource == null) {
            throw new IllegalArgumentException(
                    String.format("Test resource %s does not exist", fileName));
        }
        return resource.getFile();
    }
}
