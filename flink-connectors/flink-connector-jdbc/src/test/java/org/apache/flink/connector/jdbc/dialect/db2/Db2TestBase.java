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

package org.apache.flink.connector.jdbc.dialect.db2;

import org.apache.flink.test.util.AbstractTestBase;

import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Db2Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.stream.Stream;

import static org.junit.Assert.assertNotNull;

/** Basic class for testing DB2 jdbc. */
public class Db2TestBase extends AbstractTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(Db2TestBase.class);

    private static final DockerImageName DEBEZIUM_DOCKER_IMAGE_NAME =
            DockerImageName.parse(
                            new ImageFromDockerfile("custom/db2-cdc:1.4")
                                    .withDockerfile(getFilePath("db2_server/Dockerfile"))
                                    .get())
                    .asCompatibleSubstituteFor("ibmcom/db2");
    private static boolean db2AsnAgentRunning = false;

    protected static final Db2Container DB2_CONTAINER =
            new Db2Container(DEBEZIUM_DOCKER_IMAGE_NAME)
                    .withDatabaseName("testdb")
                    .withUsername("db2inst1")
                    .withPassword("flinkpw")
                    .withEnv("AUTOCONFIG", "false")
                    .withEnv("ARCHIVE_LOGS", "true")
                    .acceptLicense()
                    .withLogConsumer(new Slf4jLogConsumer(LOG))
                    .withLogConsumer(
                            outputFrame -> {
                                if (outputFrame
                                        .getUtf8String()
                                        .contains("The asncdc program enable finished")) {
                                    db2AsnAgentRunning = true;
                                }
                            });

    @BeforeClass
    public static void startContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(DB2_CONTAINER)).join();
        LOG.info("Containers are started.");

        LOG.info("Waiting db2 asn agent start...");
        while (!db2AsnAgentRunning) {
            try {
                Thread.sleep(5000L);
            } catch (InterruptedException e) {
                LOG.error("unexpected interrupted exception", e);
            }
        }
        LOG.info("Db2 asn agent are started.");
    }

    protected Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                DB2_CONTAINER.getJdbcUrl(),
                DB2_CONTAINER.getUsername(),
                DB2_CONTAINER.getPassword());
    }

    private static Path getFilePath(String resourceFilePath) {
        Path path = null;
        try {
            URL filePath = Db2TestBase.class.getClassLoader().getResource(resourceFilePath);
            assertNotNull("Cannot locate " + resourceFilePath, filePath);
            path = Paths.get(filePath.toURI());
        } catch (URISyntaxException e) {
            LOG.error("Cannot get path from URI.", e);
        }
        return path;
    }
}
