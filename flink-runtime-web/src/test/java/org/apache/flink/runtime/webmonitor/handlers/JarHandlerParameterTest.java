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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.core.testutils.AllCallbackWrapper;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.MessageParameter;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;
import org.apache.flink.runtime.util.BlobServerExtension;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.runtime.webmonitor.testutils.ParameterProgram;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Base test class for jar request handlers. */
abstract class JarHandlerParameterTest<
        REQB extends JarRequestBody, M extends JarMessageParameters> {
    enum ProgramArgsParType {
        String,
        List,
        Both
    }

    static final String[] PROG_ARGS = new String[] {"--host", "localhost", "--port", "1234"};
    static final int PARALLELISM = 4;

    @RegisterExtension
    private static final AllCallbackWrapper<BlobServerExtension> blobServerExtension =
            new AllCallbackWrapper<>(new BlobServerExtension());

    static final AtomicReference<JobGraph> LAST_SUBMITTED_JOB_GRAPH_REFERENCE =
            new AtomicReference<>();

    static TestingDispatcherGateway restfulGateway;
    static Path jarDir;
    static GatewayRetriever<TestingDispatcherGateway> gatewayRetriever =
            () -> CompletableFuture.completedFuture(restfulGateway);
    static CompletableFuture<String> localAddressFuture =
            CompletableFuture.completedFuture("shazam://localhost:12345");
    static Time timeout = Time.seconds(10);
    static Map<String, String> responseHeaders = Collections.emptyMap();

    private static Path jarWithManifest;
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
                                    LAST_SUBMITTED_JOB_GRAPH_REFERENCE.set(jobGraph);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .build();

        gatewayRetriever = () -> CompletableFuture.completedFuture(restfulGateway);
        localAddressFuture = CompletableFuture.completedFuture("shazam://localhost:12345");
        timeout = Time.seconds(10);
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
    void testConfigurationViaQueryParametersWithProgArgsAsString() throws Exception {
        testConfigurationViaQueryParameters(ProgramArgsParType.String);
    }

    @Test
    void testConfigurationViaQueryParametersWithProgArgsAsList() throws Exception {
        testConfigurationViaQueryParameters(ProgramArgsParType.List);
    }

    @Test
    void testConfigurationViaQueryParametersFailWithProgArgsAsStringAndList() throws Exception {
        assertThatThrownBy(() -> testConfigurationViaQueryParameters(ProgramArgsParType.Both))
                .isInstanceOf(RestHandlerException.class)
                .satisfies(
                        e ->
                                assertThat(((RestHandlerException) e).getHttpResponseStatus())
                                        .isEqualTo(HttpResponseStatus.BAD_REQUEST));
    }

    private void testConfigurationViaQueryParameters(ProgramArgsParType programArgsParType)
            throws Exception {
        // configure submission via query parameters
        handleRequest(
                createRequest(
                        getDefaultJarRequestBody(),
                        getJarMessageParameters(programArgsParType),
                        getUnresolvedJarMessageParameters(),
                        jarWithoutManifest));
        validateGraph();
    }

    @Test
    void testConfigurationViaJsonRequestWithProgArgsAsString() throws Exception {
        testConfigurationViaJsonRequest(ProgramArgsParType.String);
    }

    @Test
    void testConfigurationViaJsonRequestWithProgArgsAsList() throws Exception {
        testConfigurationViaJsonRequest(ProgramArgsParType.List);
    }

    @Test
    void testConfigurationViaJsonRequestFailWithProgArgsAsStringAndList() throws Exception {
        assertThatThrownBy(() -> testConfigurationViaJsonRequest(ProgramArgsParType.Both))
                .isInstanceOf(RestHandlerException.class)
                .satisfies(
                        e ->
                                assertThat(((RestHandlerException) e).getHttpResponseStatus())
                                        .isEqualTo(HttpResponseStatus.BAD_REQUEST));
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
        validateGraphWithFlinkConfig(LAST_SUBMITTED_JOB_GRAPH_REFERENCE.get());
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

        Optional<JobGraph> jobGraph = getLastSubmittedJobGraphAndReset();

        assertThat(jobGraph.isPresent()).isTrue();
        assertThat(jobGraph.get().getJobID()).isEqualTo(jobId);
    }

    private void testConfigurationViaJsonRequest(ProgramArgsParType programArgsParType)
            throws Exception {
        handleRequest(
                createRequest(
                        getJarRequestBody(programArgsParType),
                        getUnresolvedJarMessageParameters(),
                        getUnresolvedJarMessageParameters(),
                        jarWithoutManifest));
        validateGraph();
    }

    @Test
    void testParameterPrioritizationWithProgArgsAsString() throws Exception {
        testParameterPrioritization(ProgramArgsParType.String);
    }

    @Test
    void testParameterPrioritizationWithProgArgsAsList() throws Exception {
        testParameterPrioritization(ProgramArgsParType.List);
    }

    @Test
    void testFailIfProgArgsAreAsStringAndAsList() throws Exception {
        assertThatThrownBy(() -> testParameterPrioritization(ProgramArgsParType.Both))
                .isInstanceOf(RestHandlerException.class)
                .satisfies(
                        e ->
                                assertThat(((RestHandlerException) e).getHttpResponseStatus())
                                        .isEqualTo(HttpResponseStatus.BAD_REQUEST));
    }

    private void testParameterPrioritization(ProgramArgsParType programArgsParType)
            throws Exception {
        // configure submission via query parameters and JSON request, JSON should be prioritized
        handleRequest(
                createRequest(
                        getJarRequestBody(programArgsParType),
                        getWrongJarMessageParameters(programArgsParType),
                        getUnresolvedJarMessageParameters(),
                        jarWithoutManifest));
        validateGraph();
    }

    static String getProgramArgsString(ProgramArgsParType programArgsParType) {
        return programArgsParType == ProgramArgsParType.String
                        || programArgsParType == ProgramArgsParType.Both
                ? String.join(" ", PROG_ARGS)
                : null;
    }

    static List<String> getProgramArgsList(ProgramArgsParType programArgsParType) {
        return programArgsParType == ProgramArgsParType.List
                        || programArgsParType == ProgramArgsParType.Both
                ? Arrays.asList(PROG_ARGS)
                : null;
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

    abstract M getJarMessageParameters(ProgramArgsParType programArgsParType);

    abstract M getWrongJarMessageParameters(ProgramArgsParType programArgsParType);

    abstract REQB getDefaultJarRequestBody();

    abstract REQB getJarRequestBody(ProgramArgsParType programArgsParType);

    abstract REQB getJarRequestBodyWithJobId(JobID jobId);

    abstract REQB getJarRequestWithConfiguration();

    abstract void handleRequest(HandlerRequest<REQB> request) throws Exception;

    JobGraph validateDefaultGraph() {
        JobGraph jobGraph = LAST_SUBMITTED_JOB_GRAPH_REFERENCE.getAndSet(null);
        assertThat(ParameterProgram.actualArguments).isEmpty();
        assertThat(getExecutionConfig(jobGraph).getParallelism())
                .isEqualTo(CoreOptions.DEFAULT_PARALLELISM.defaultValue().intValue());
        return jobGraph;
    }

    JobGraph validateGraph() {
        JobGraph jobGraph = LAST_SUBMITTED_JOB_GRAPH_REFERENCE.getAndSet(null);
        assertThat(ParameterProgram.actualArguments).isEqualTo(PROG_ARGS);
        assertThat(getExecutionConfig(jobGraph).getParallelism()).isEqualTo(PARALLELISM);
        return jobGraph;
    }

    abstract void validateGraphWithFlinkConfig(JobGraph jobGraph);

    private static Optional<JobGraph> getLastSubmittedJobGraphAndReset() {
        return Optional.ofNullable(LAST_SUBMITTED_JOB_GRAPH_REFERENCE.getAndSet(null));
    }

    static ExecutionConfig getExecutionConfig(JobGraph jobGraph) {
        ExecutionConfig executionConfig;
        try {
            executionConfig =
                    jobGraph.getSerializedExecutionConfig()
                            .deserializeValue(ParameterProgram.class.getClassLoader());
        } catch (Exception e) {
            throw new AssertionError("Exception while deserializing ExecutionConfig.", e);
        }
        return executionConfig;
    }
}
