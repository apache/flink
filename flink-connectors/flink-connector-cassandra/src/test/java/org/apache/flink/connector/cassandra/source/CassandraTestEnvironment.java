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

package org.apache.flink.connector.cassandra.source;

import org.apache.flink.connector.testframe.TestResource;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.util.DockerImageVersions;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

/**
 * Junit test environment that contains everything needed at the test suite level: testContainer
 * setup, keyspace setup, Cassandra cluster/session management ClusterBuilder setup).
 */
@Testcontainers
public class CassandraTestEnvironment implements TestResource {
    private static final int PORT = 9042;

    private static final int MAX_CONNECTION_RETRY = 3;
    private static final long CONNECTION_RETRY_DELAY = 500L;

    static final String KEYSPACE = "flink";

    private static final String CREATE_KEYSPACE_QUERY =
            "CREATE KEYSPACE "
                    + KEYSPACE
                    + " WITH replication= {'class':'SimpleStrategy', 'replication_factor':1};";

    @Container
    private static final CassandraContainer CASSANDRA_CONTAINER = createCassandraContainer();

    @TempDir private File tempDir;

    private static Cluster cluster;
    private static Session session;
    private ClusterBuilder clusterBuilder;

    private static final Logger LOG = LoggerFactory.getLogger(CassandraTestEnvironment.class);

    private static final int READ_TIMEOUT_MILLIS = 36000;

    @Override
    public void startUp() throws Exception {
        startEnv();
    }

    @Override
    public void tearDown() throws Exception {
        stopEnv();
    }

    private void startEnv() {
        // need to start the container to be able to patch the configuration file inside the
        // container
        // CASSANDRA_CONTAINER#start() already contains retrials
        CASSANDRA_CONTAINER.start();
        CASSANDRA_CONTAINER.followOutput(
                new Slf4jLogConsumer(LOG),
                OutputFrame.OutputType.END,
                OutputFrame.OutputType.STDERR,
                OutputFrame.OutputType.STDOUT);
        raiseCassandraRequestsTimeouts();
        // restart the container so that the new timeouts are taken into account
        CASSANDRA_CONTAINER.stop();
        CASSANDRA_CONTAINER.start();
        cluster = CASSANDRA_CONTAINER.getCluster();
        clusterBuilder = createBuilderWithConsistencyLevel(ConsistencyLevel.ONE);

        int retried = 0;
        while (retried < MAX_CONNECTION_RETRY) {
            try {
                session = cluster.connect();
                break;
            } catch (NoHostAvailableException e) {
                retried++;
                LOG.debug(
                        "Connection failed with NoHostAvailableException : retry number {}, will retry to connect within {} ms",
                        retried,
                        CONNECTION_RETRY_DELAY);
                if (retried == MAX_CONNECTION_RETRY) {
                    throw new RuntimeException(
                            String.format(
                                    "Failed to connect to Cassandra cluster after %d retries every %d ms",
                                    retried, CONNECTION_RETRY_DELAY),
                            e);
                }
                try {
                    Thread.sleep(CONNECTION_RETRY_DELAY);
                } catch (InterruptedException ignored) {
                }
            }
        }
        session.execute(requestWithTimeout(CREATE_KEYSPACE_QUERY));
    }

    private void stopEnv() {
        if (session != null) {
            session.close();
        }
        if (cluster != null) {
            cluster.close();
        }
        CASSANDRA_CONTAINER.stop();
    }

    private ClusterBuilder createBuilderWithConsistencyLevel(ConsistencyLevel consistencyLevel) {
        return new ClusterBuilder() {
            @Override
            protected Cluster buildCluster(Cluster.Builder builder) {
                return builder.addContactPointsWithPorts(
                                new InetSocketAddress(
                                        CASSANDRA_CONTAINER.getHost(),
                                        CASSANDRA_CONTAINER.getMappedPort(PORT)))
                        .withQueryOptions(
                                new QueryOptions()
                                        .setConsistencyLevel(consistencyLevel)
                                        .setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL))
                        .withSocketOptions(
                                new SocketOptions()
                                        // default timeout x 3
                                        .setConnectTimeoutMillis(15000)
                                        // default timeout x3 and higher than
                                        // request_timeout_in_ms at the cluster level
                                        .setReadTimeoutMillis(READ_TIMEOUT_MILLIS))
                        .withoutJMXReporting()
                        .withoutMetrics()
                        .build();
            }
        };
    }

    public static CassandraContainer createCassandraContainer() {
        CassandraContainer cassandra = new CassandraContainer(DockerImageVersions.CASSANDRA_4_0);
        cassandra.withJmxReporting(false);
        return cassandra;
    }

    private void raiseCassandraRequestsTimeouts() {
        final File tempConfiguration = new File(tempDir, "configuration");
        try {
            CASSANDRA_CONTAINER.copyFileFromContainer(
                    "/etc/cassandra/cassandra.yaml", tempConfiguration.getAbsolutePath());
            String configuration =
                    new String(
                            Files.readAllBytes(tempConfiguration.toPath()), StandardCharsets.UTF_8);
            String patchedConfiguration =
                    configuration
                            .replaceAll(
                                    "request_timeout_in_ms: [0-9]+",
                                    "request_timeout_in_ms: 30000") // x3 default timeout
                            .replaceAll(
                                    "read_request_timeout_in_ms: [0-9]+",
                                    "read_request_timeout_in_ms: 15000") // x3 default timeout
                            .replaceAll(
                                    "write_request_timeout_in_ms: [0-9]+",
                                    "write_request_timeout_in_ms: 6000"); // x3 default timeout
            CASSANDRA_CONTAINER.copyFileToContainer(
                    Transferable.of(patchedConfiguration.getBytes(StandardCharsets.UTF_8)),
                    "/etc/cassandra/cassandra.yaml");
        } catch (IOException e) {
            throw new RuntimeException("Unable to open Cassandra configuration file ", e);
        } finally {
            tempConfiguration.delete();
        }
    }

    static Statement requestWithTimeout(String query) {
        return new SimpleStatement(query).setReadTimeoutMillis(READ_TIMEOUT_MILLIS);
    }

    public ClusterBuilder getClusterBuilder() {
        return clusterBuilder;
    }

    public Session getSession() {
        return session;
    }
}
