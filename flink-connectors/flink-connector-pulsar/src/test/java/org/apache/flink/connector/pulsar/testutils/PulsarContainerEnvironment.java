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

package org.apache.flink.connector.pulsar.testutils;

import org.apache.flink.connectors.test.common.TestResource;
import org.apache.flink.connectors.test.common.junit.annotations.ExternalSystem;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

import static org.apache.flink.util.DockerImageVersions.PULSAR;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.testcontainers.containers.PulsarContainer.BROKER_HTTP_PORT;

/**
 * A JUnit 5 {@link Extension} for supporting running of pulsar test container. This class is also a
 * {@link ExternalSystem} for {@code flink-connector-testing} tools.
 *
 * <p>Some old flink tests are based on JUint 4, this class is also support it. The follow code
 * snippet shows how to use this class in JUnit 4.
 *
 * <pre>{@code
 * @ClassRule
 * public static PulsarContainerEnvironment environment = new PulsarContainerEnvironment();
 * }</pre>
 *
 * <p>If you want to use this class in JUnit 5, just simply extends {@link PulsarTestSuiteBase}, all
 * the helper methods in {@code PulsarContainerOperator} is also exposing there.
 */
public class PulsarContainerEnvironment extends ExternalResource
        implements BeforeAllCallback, AfterAllCallback, TestResource {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarContainerEnvironment.class);

    private PulsarContainer container;
    private PulsarContainerOperator operator;

    /** JUnit 4 Rule setup method. */
    @Override
    protected void before() {
        startUp();
    }

    /** JUnit 5 Extension setup method. */
    @Override
    public void beforeAll(ExtensionContext context) {
        startUp();
    }

    /** flink-connector-testing setup method. */
    @Override
    public void startUp() {
        DockerImageName imageName = DockerImageName.parse(PULSAR);
        this.container = new PulsarContainer(imageName);

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

        // Start Pulsar Container.
        container.start();
        container.followOutput(new Slf4jLogConsumer(LOG).withSeparateOutputStreams());

        this.operator =
                new PulsarContainerOperator(
                        container.getPulsarBrokerUrl(), container.getHttpServiceUrl());
    }

    /** JUnit 4 Rule shutdown method. */
    @Override
    protected void after() {
        try {
            tearDown();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** JUnit 5 Extension shutdown method. */
    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        tearDown();
    }

    /** flink-connector-testing shutdown method. */
    @Override
    public void tearDown() throws Exception {
        container.stop();
        operator.close();
    }

    /** Get a common supported set of method for operating pulsar which is in container. */
    public PulsarContainerOperator operator() {
        return checkNotNull(operator, "You should start the pulsar container first.");
    }
}
