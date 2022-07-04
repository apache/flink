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

package org.apache.flink.table.gateway;

import org.apache.flink.tests.util.TestUtils;
import org.apache.flink.tests.util.flink.ClusterController;
import org.apache.flink.tests.util.flink.FlinkResource;
import org.apache.flink.tests.util.flink.FlinkResourceSetup;
import org.apache.flink.tests.util.flink.JarLocation;
import org.apache.flink.tests.util.flink.LocalStandaloneFlinkResource;
import org.apache.flink.tests.util.flink.LocalStandaloneFlinkResourceFactory;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;

public class SQLGatewayITCase extends TestLogger {

    private static String JDBC_URL = "jdbc:hive2://localhost:8084/default;auth=noSasl";
    private static String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";

    private static final Path HIVE_SQL_CONNECOTR_JAR =
            TestUtils.getResource(".*dependencies/flink-sql-connector-hive-.*.jar");

    @Rule
    public final FlinkResource flink =
            new LocalStandaloneFlinkResourceFactory()
                    .create(
                            FlinkResourceSetup.builder()
                                    .addJar(HIVE_SQL_CONNECOTR_JAR, JarLocation.LIB)
                                    .build());

    @Test
    public void testGateway() throws Exception {
        try (ClusterController clusterController = flink.startCluster(1)) {
            ((LocalStandaloneFlinkResource) flink)
                    .startSQLGateway("-Dsql-gateway.endpoint.type=hiveserver2");
            Thread.sleep(2000);
            Class.forName(DRIVER_NAME);
            try {
                DriverManager.getConnection(JDBC_URL);
            } catch (SQLException e) {
                assertThat(e.getMessage())
                        .contains(
                                "Embedded metastore is not allowed. Make sure you have set a valid value for hive.metastore.uris");
            }
        }
    }
}
