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

package org.apache.flink.connector.pulsar.testutils.runtime.container;

import org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntime;
import org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntimeOperator;
import org.apache.flink.util.DockerImageVersions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.time.Duration;

import static org.apache.flink.util.DockerImageVersions.PULSAR;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.testcontainers.containers.PulsarContainer.BROKER_HTTP_PORT;
import static org.testcontainers.containers.PulsarContainer.BROKER_PORT;

/**
 * {@link PulsarRuntime} implementation, use the TestContainers as the backend. We would start a
 * pulsar container by this provider.
 */
public class PulsarContainerRuntime implements PulsarRuntime {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarContainerRuntime.class);
    private static final String PULSAR_INTERNAL_HOSTNAME = "pulsar";

    // This url is used on the container side.
    public static final String PULSAR_SERVICE_URL =
            String.format("pulsar://%s:%d", PULSAR_INTERNAL_HOSTNAME, BROKER_PORT);
    // This url is used on the container side.
    public static final String PULSAR_ADMIN_URL =
            String.format("http://%s:%d", PULSAR_INTERNAL_HOSTNAME, BROKER_HTTP_PORT);

    /**
     * Create a pulsar container provider by a predefined version, this constance {@link
     * DockerImageVersions#PULSAR} should be bumped after the new pulsar release.
     */
    private final PulsarContainer container = new PulsarContainer(DockerImageName.parse(PULSAR));

    private PulsarRuntimeOperator operator;

    public PulsarContainerRuntime bindWithFlinkContainer(GenericContainer<?> flinkContainer) {
        this.container
                .withNetworkAliases(PULSAR_INTERNAL_HOSTNAME)
                .dependsOn(flinkContainer)
                .withNetwork(flinkContainer.getNetwork());
        return this;
    }

    @Override
    public void startUp() {
        // Prepare Pulsar Container.
        container.withClasspathResourceMapping(
                "containers/txnStandalone.conf",
                "/pulsar/conf/standalone.conf",
                BindMode.READ_ONLY);
        container.addExposedPort(2181);
        container.waitingFor(
                new HttpWaitStrategy()
                        .forPort(BROKER_HTTP_PORT)
                        .forStatusCode(200)
                        .forPath("/admin/v2/namespaces/public/default")
                        .withStartupTimeout(Duration.ofMinutes(5)));

        // Start the Pulsar Container.
        container.start();
        container.followOutput(new Slf4jLogConsumer(LOG).withSeparateOutputStreams());

        // Create the operator.
        this.operator =
                new PulsarRuntimeOperator(
                        container.getPulsarBrokerUrl(), container.getHttpServiceUrl());
    }

    @Override
    public void tearDown() {
        try {
            operator.close();
            this.operator = null;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        container.stop();
    }

    @Override
    public PulsarRuntimeOperator operator() {
        return checkNotNull(operator, "You should start this pulsar container first.");
    }
}
