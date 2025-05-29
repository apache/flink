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

package org.apache.flink.runtime.webmonitor.handlers;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.core.testutils.AllCallbackWrapper;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.messages.MessageParameter;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;
import org.apache.flink.runtime.util.BlobServerExtension;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.runtime.webmonitor.testutils.ParameterProgram;
import org.apache.flink.streaming.api.graph.ExecutionPlan;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Base test class for jar request handlers. */
abstract class JarHandlerParameterTest<
        REQB extends JarRequestBody, M extends JarMessageParameters> {
    static final String[] PROG_ARGS = new String[] {"--host", "localhost", "--port", "1234"};
    static final int PARALLELISM = 4;

    @RegisterExtension
    private static final AllCallbackWrapper<BlobServerExtension> blobServerExtension =
            new AllCallbackWrapper<>(new BlobServerExtension());

    static final AtomicReference<ExecutionPlan> LAST_SUBMITTED_EXECUTION_PLAN_REFERENCE =
            new AtomicReference<>();

    static TestingDispatcherGateway restfulGateway;
    static Path jarDir;
    static GatewayRetriever<TestingDispatcherGateway> gatewayRetriever =
            () -> CompletableFuture.completedFuture(restfulGateway);
    static CompletableFuture<String> localAddressFuture =
            CompletableFuture.completedFuture("shazam://localhost:12345");
    static Duration timeout = Duration.ofSeconds(10);
    static Map<String, String> responseHeaders = Collections.emptyMap();

    protected static Path jarWithManifest;
    private static Path jarWithoutManifest;

    static void init(File tmpDir) throws Exception {
        jarDir = tmpDir.toPath();

        // properties are set property by surefire plugin
        final String parameterProgramJarName = System.getProperty("parameterJarName") + ".jar";
        final String parameterProgramWithoutManifestJarName =
                System.getProperty("parameterJarWithoutManifestName") + ".jar";
        final Path jarLocation = Paths.get(System.getProperty("targetDir"));

        jarWithManifest =
                Files.copy(
                        jarLocation.resolve(parameterProgramJarName),
                        jarDir.resolve("program-with-manifest.jar"));
        jarWithoutManifest =
                Files.copy(
                        jarLocation.resolve(parameterProgramWithoutManifestJarName),
                        jarDir.resolve("program-without-manifest.jar"));

        restfulGateway =
                TestingDispatcherGateway.newBuilder()
                        .setBlobServerPort(
                                blobServerExtension.getCustomExtension().getBlobServerPort())
                        .setSubmitFunction(
                                jobGraph -> {
                                    LAST_SUBMITTED_EXECUTION_PLAN_REFERENCE.set(jobGraph);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .build();

        gatewayRetriever = () -> CompletableFuture.completedFuture(restfulGateway);
        localAddressFuture = CompletableFuture.completedFuture("shazam://localhost:12345");
        timeout = Duration.ofSeconds(10);
        responseHeaders = Collections.emptyMap();
    }

    @BeforeEach
    void reset() {
        ParameterProgram.actualArguments = null;
    }

    @Test
    void testDefaultParameters() throws Exception {
        // baseline, ensure that reasonable defaults are chosen
        handleRequest(
                createRequest(
                        getDefaultJarRequestBody(),
                        getUnresolvedJarMessageParameters(),
                        getUnresolvedJarMessageParameters(),
                        jarWithManifest));
        validateDefaultGraph();
    }

    @Test
    void testConfigurationViaQueryParametersWithProgArgs() throws Exception {
        testConfigurationViaQueryParameters();
    }

    private void testConfigurationViaQueryParameters() throws Exception {
        // configure submission via query parameters
        handleRequest(
                createRequest(
                        getDefaultJarRequestBody(),
                        getJarMessageParameters(),
                        getUnresolvedJarMessageParameters(),
                        jarWithoutManifest));
        validateGraph();
    }

    @Test
    void testConfigurationViaJsonRequestWithProgArgs() throws Exception {
        testConfigurationViaJsonRequest();
    }

    @Test
    void testConfigurationViaConfiguration() throws Exception {
        final REQB requestBody = getJarRequestWithConfiguration();
        handleRequest(
                createRequest(
                        requestBody,
                        getUnresolvedJarMessageParameters(),
                        getUnresolvedJarMessageParameters(),
                        jarWithManifest));
        validateGraphWithFlinkConfig(LAST_SUBMITTED_EXECUTION_PLAN_REFERENCE.get());
    }

    @Test
    void testProvideJobId() throws Exception {
        JobID jobId = new JobID();

        HandlerRequest<REQB> request =
                createRequest(
                        getJarRequestBodyWithJobId(jobId),
                        getUnresolvedJarMessageParameters(),
                        getUnresolvedJarMessageParameters(),
                        jarWithManifest);

        handleRequest(request);

        Optional<ExecutionPlan> executionPlan = getLastSubmittedJobGraphAndReset();

        assertThat(executionPlan.isPresent()).isTrue();
        assertThat(executionPlan.get().getJobID()).isEqualTo(jobId);
    }

    private void testConfigurationViaJsonRequest() throws Exception {
        handleRequest(
                createRequest(
                        getJarRequestBody(),
                        getUnresolvedJarMessageParameters(),
                        getUnresolvedJarMessageParameters(),
                        jarWithoutManifest));
        validateGraph();
    }

    @Test
    void testParameterPrioritizationWithProgArgs() throws Exception {
        testParameterPrioritization();
    }

    private void testParameterPrioritization() throws Exception {
        // configure submission via query parameters and JSON request, JSON should be prioritized
        handleRequest(
                createRequest(
                        getJarRequestBody(),
                        getWrongJarMessageParameters(),
                        getUnresolvedJarMessageParameters(),
                        jarWithoutManifest));
        validateGraph();
    }

    protected static <REQB extends JarRequestBody, M extends JarMessageParameters>
            HandlerRequest<REQB> createRequest(
                    REQB requestBody, M parameters, M unresolvedMessageParameters, Path jar)
                    throws HandlerRequestException {

        final Map<String, List<String>> queryParameterAsMap =
                parameters.getQueryParameters().stream()
                        .filter(MessageParameter::isResolved)
                        .collect(
                                Collectors.toMap(
                                        MessageParameter::getKey,
                                        JarHandlerParameterTest::getValuesAsString));

        return HandlerRequest.resolveParametersAndCreate(
                requestBody,
                unresolvedMessageParameters,
                Collections.singletonMap(JarIdPathParameter.KEY, jar.getFileName().toString()),
                queryParameterAsMap,
                Collections.emptyList());
    }

    private static <X> List<String> getValuesAsString(MessageQueryParameter<X> parameter) {
        final List<X> values = parameter.getValue();
        return values.stream().map(parameter::convertValueToString).collect(Collectors.toList());
    }

    abstract M getUnresolvedJarMessageParameters();

    abstract M getJarMessageParameters();

    abstract M getWrongJarMessageParameters();

    abstract REQB getDefaultJarRequestBody();

    abstract REQB getJarRequestBody();

    abstract REQB getJarRequestBodyWithJobId(JobID jobId);

    abstract REQB getJarRequestWithConfiguration();

    abstract void handleRequest(HandlerRequest<REQB> request) throws Exception;

    ExecutionPlan validateDefaultGraph() throws Exception {
        ExecutionPlan executionPlan = LAST_SUBMITTED_EXECUTION_PLAN_REFERENCE.getAndSet(null);

        assertThat(ParameterProgram.actualArguments).isEmpty();
        assertThat(getExecutionConfig(executionPlan).getParallelism())
                .isEqualTo(CoreOptions.DEFAULT_PARALLELISM.defaultValue().intValue());
        return executionPlan;
    }

    ExecutionPlan validateGraph() throws Exception {
        ExecutionPlan executionPlan = LAST_SUBMITTED_EXECUTION_PLAN_REFERENCE.getAndSet(null);

        assertThat(ParameterProgram.actualArguments).isEqualTo(PROG_ARGS);
        assertThat(getExecutionConfig(executionPlan).getParallelism()).isEqualTo(PARALLELISM);
        return executionPlan;
    }

    abstract void validateGraphWithFlinkConfig(ExecutionPlan executionPlan);

    private static Optional<ExecutionPlan> getLastSubmittedJobGraphAndReset() {
        return Optional.ofNullable(LAST_SUBMITTED_EXECUTION_PLAN_REFERENCE.getAndSet(null));
    }

    static ExecutionConfig getExecutionConfig(ExecutionPlan executionPlan) {
        ExecutionConfig executionConfig;
        try {
            executionConfig =
                    executionPlan
                            .getSerializedExecutionConfig()
                            .deserializeValue(ParameterProgram.class.getClassLoader());
        } catch (Exception e) {
            throw new AssertionError("Exception while deserializing ExecutionConfig.", e);
        }
        return executionConfig;
    }
}
