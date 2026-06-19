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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.streaming.api.graph.ExecutionPlan;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.rest.header.application.DeployScriptHeaders;
import org.apache.flink.table.gateway.rest.header.session.OpenSessionHeaders;
import org.apache.flink.table.gateway.rest.message.application.DeployScriptRequestBody;
import org.apache.flink.table.gateway.rest.message.session.OpenSessionRequestBody;
import org.apache.flink.table.gateway.rest.message.session.SessionMessageParameters;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestEndpointExtension;
import org.apache.flink.table.gateway.rest.util.TestingRestClient;
import org.apache.flink.table.gateway.service.utils.MockHttpServer;
import org.apache.flink.table.gateway.service.utils.SqlGatewayServiceExtension;
import org.apache.flink.table.runtime.application.SqlDriver;
import org.apache.flink.util.FileUtils;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.File;
import java.net.URL;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** ITCase to test deploy the script into application mode. */
public class DeployScriptITCase {

    @RegisterExtension
    @Order(1)
    public static final SqlGatewayServiceExtension SQL_GATEWAY_SERVICE_EXTENSION =
            new SqlGatewayServiceExtension(Configuration::new);

    @RegisterExtension
    @Order(2)
    private static final SqlGatewayRestEndpointExtension SQL_GATEWAY_REST_ENDPOINT_EXTENSION =
            new SqlGatewayRestEndpointExtension(SQL_GATEWAY_SERVICE_EXTENSION::getService);

    private static TestingRestClient restClient;
    private static SessionHandle sessionHandle;
    private static final String script =
            "CREATE TEMPORARY TABLE sink(\n"
                    + "  a INT\n"
                    + ") WITH (\n"
                    + "  'connector' = 'blackhole'\n"
                    + ");\n"
                    + "INSERT INTO sink VALUES (1);";

    @BeforeAll
    static void beforeAll() throws Exception {
        restClient = TestingRestClient.getTestingRestClient();
        sessionHandle =
                new SessionHandle(
                        UUID.fromString(
                                restClient
                                        .sendRequest(
                                                SQL_GATEWAY_REST_ENDPOINT_EXTENSION
                                                        .getTargetAddress(),
                                                SQL_GATEWAY_REST_ENDPOINT_EXTENSION.getTargetPort(),
                                                OpenSessionHeaders.getInstance(),
                                                EmptyMessageParameters.getInstance(),
                                                new OpenSessionRequestBody(
                                                        "test",
                                                        Collections.singletonMap("key", "value")))
                                        .get()
                                        .getSessionHandle()));
    }

    @Test
    void testDeployScriptToYarnCluster(@TempDir Path workDir) throws Exception {
        verifyDeployScriptToCluster("yarn-application", script, null, script);
        try (MockHttpServer server = MockHttpServer.startHttpServer()) {
            File file = workDir.resolve("script.sql").toFile();
            assertThat(file.createNewFile()).isTrue();
            FileUtils.writeFileUtf8(file, script);
            URL url = server.prepareResource("/download/script.sql", file);
            verifyDeployScriptToCluster("yarn-application", null, url.toString(), script);
        }
    }

    @Test
    void testDeployScriptToKubernetesCluster(@TempDir Path workDir) throws Exception {
        verifyDeployScriptToCluster("kubernetes-application", script, null, script);
        try (MockHttpServer server = MockHttpServer.startHttpServer()) {
            File file = workDir.resolve("script.sql").toFile();
            assertThat(file.createNewFile()).isTrue();
            FileUtils.writeFileUtf8(file, script);
            URL url = server.prepareResource("/download/script.sql", file);
            verifyDeployScriptToCluster("kubernetes-application", null, url.toString(), script);
        }
    }

    private void verifyDeployScriptToCluster(
            String target, @Nullable String script, @Nullable String scriptUri, String content)
            throws Exception {
        TestApplicationClusterClientFactory.id = target;

        assertThat(
                        restClient
                                .sendRequest(
                                        SQL_GATEWAY_REST_ENDPOINT_EXTENSION.getTargetAddress(),
                                        SQL_GATEWAY_REST_ENDPOINT_EXTENSION.getTargetPort(),
                                        DeployScriptHeaders.getInstance(),
                                        new SessionMessageParameters(sessionHandle),
                                        new DeployScriptRequestBody(
                                                script,
                                                scriptUri,
                                                Collections.singletonMap(
                                                        DeploymentOptions.TARGET.key(), target)))
                                .get()
                                .getClusterID())
                .isEqualTo("test");
        ApplicationConfiguration config = TestApplicationClusterDescriptor.applicationConfiguration;
        assertThat(TestApplicationClusterClientFactory.configuration.getString("key", "none"))
                .isEqualTo("value");
        assertThat(config.getApplicationClassName()).isEqualTo(SqlDriver.class.getName());
        assertThat(SqlDriver.parseOptions(config.getProgramArguments())).isEqualTo(content);
    }

    /**
     * Test {@link ClusterClientFactory} to capture {@link Configuration} and {@link
     * ApplicationConfiguration}.
     */
    @SuppressWarnings({"unchecked", "rawTypes"})
    public static class TestApplicationClusterClientFactory<ClusterID>
            implements ClusterClientFactory {

        public static String id;

        private static volatile Configuration configuration;

        @Override
        public boolean isCompatibleWith(Configuration configuration) {
            return Objects.equals(id, configuration.get(DeploymentOptions.TARGET));
        }

        @Override
        @SuppressWarnings("unchecked")
        public ClusterDescriptor<ClusterID> createClusterDescriptor(Configuration configuration) {
            TestApplicationClusterClientFactory.configuration = configuration;
            return TestApplicationClusterDescriptor.INSTANCE;
        }

        @Override
        @Nullable
        public String getClusterId(Configuration configuration) {
            return "test-application";
        }

        @Override
        public ClusterSpecification getClusterSpecification(Configuration configuration) {
            return new ClusterSpecification.ClusterSpecificationBuilder()
                    .createClusterSpecification();
        }
    }

    private static class TestApplicationClusterDescriptor<T> implements ClusterDescriptor<T> {

        @SuppressWarnings("rawTypes")
        private static final TestApplicationClusterDescriptor INSTANCE =
                new TestApplicationClusterDescriptor<>();

        static volatile ApplicationConfiguration applicationConfiguration;

        private TestApplicationClusterDescriptor() {}

        @Override
        public String getClusterDescription() {
            return "Test Application Cluster Descriptor";
        }

        @Override
        public ClusterClientProvider<T> retrieve(T clusterId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ClusterClientProvider<T> deploySessionCluster(
                ClusterSpecification clusterSpecification) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ClusterClientProvider<T> deployApplicationCluster(
                final ClusterSpecification clusterSpecification,
                final ApplicationConfiguration applicationConfiguration) {
            TestApplicationClusterDescriptor.applicationConfiguration = applicationConfiguration;
            return new ClusterClientProvider<T>() {
                @Override
                public ClusterClient<T> getClusterClient() {
                    return new TestClusterClient();
                }
            };
        }

        @Override
        public void killCluster(T clusterId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            // nothing to do
        }
    }

    @SuppressWarnings("rawTypes")
    private static class TestClusterClient implements ClusterClient {

        @Override
        public void close() {}

        @Override
        public Object getClusterId() {
            return "test";
        }

        @Override
        public Configuration getFlinkConfiguration() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void shutDownCluster() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getWebInterfaceURL() {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Collection<JobStatusMessage>> listJobs() {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Acknowledge> disposeSavepoint(String savepointPath) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<JobID> submitJob(ExecutionPlan executionPlan) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<JobStatus> getJobStatus(JobID jobId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<JobResult> requestJobResult(JobID jobId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Map<String, Object>> getAccumulators(
                JobID jobID, ClassLoader loader) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Acknowledge> cancel(JobID jobId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<String> cancelWithSavepoint(
                JobID jobId, @Nullable String savepointDirectory, SavepointFormatType formatType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<String> stopWithSavepoint(
                JobID jobId,
                boolean advanceToEndOfEventTime,
                @Nullable String savepointDirectory,
                SavepointFormatType formatType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<String> stopWithDetachedSavepoint(
                JobID jobId,
                boolean advanceToEndOfEventTime,
                @Nullable String savepointDirectory,
                SavepointFormatType formatType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<String> triggerSavepoint(
                JobID jobId, @Nullable String savepointDirectory, SavepointFormatType formatType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Long> triggerCheckpoint(
                JobID jobId, CheckpointType checkpointType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<String> triggerDetachedSavepoint(
                JobID jobId, @Nullable String savepointDirectory, SavepointFormatType formatType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<CoordinationResponse> sendCoordinationRequest(
                JobID jobId, String operatorUid, CoordinationRequest request) {
            throw new UnsupportedOperationException();
        }
    }
}
