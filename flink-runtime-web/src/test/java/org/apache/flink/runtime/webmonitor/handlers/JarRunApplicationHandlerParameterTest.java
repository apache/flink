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

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.deployment.application.PackagedProgramApplication;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.execution.RecoveryClaimMode;
import org.apache.flink.core.testutils.AllCallbackWrapper;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.messages.MessageParameter;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;
import org.apache.flink.runtime.util.BlobServerExtension;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.runtime.webmonitor.testutils.ParameterProgram;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.concurrent.FutureUtils;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the parameter handling of the {@link JarRunApplicationHandler}. */
class JarRunApplicationHandlerParameterTest {
    static final String[] PROG_ARGS = new String[] {"--host", "localhost", "--port", "1234"};
    static final int PARALLELISM = 4;
    static final boolean ALLOW_NON_RESTORED_STATE_QUERY = true;
    static final String RESTORE_PATH = "/foo/bar";
    static final RecoveryClaimMode RESTORE_MODE = RecoveryClaimMode.CLAIM;

    @RegisterExtension
    private static final AllCallbackWrapper<BlobServerExtension> blobServerExtension =
            new AllCallbackWrapper<>(new BlobServerExtension());

    static final AtomicReference<PackagedProgramApplication> LAST_SUBMITTED_APPLICATION_REFERENCE =
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

    private static JarRunApplicationHandler handler;

    private static final Configuration FLINK_CONFIGURATION =
            new Configuration()
                    .set(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT, Duration.ofMillis(120000L))
                    .set(CoreOptions.DEFAULT_PARALLELISM, 57)
                    .set(StateRecoveryOptions.SAVEPOINT_PATH, "/foo/bar/test")
                    .set(StateRecoveryOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE, false)
                    .set(StateRecoveryOptions.RESTORE_MODE, RESTORE_MODE)
                    .set(
                            PipelineOptions.PARALLELISM_OVERRIDES,
                            new HashMap<>() {
                                {
                                    put("v1", "10");
                                }
                            });

    @BeforeAll
    static void setup(@TempDir File tempDir) throws Exception {
        init(tempDir);
        final GatewayRetriever<TestingDispatcherGateway> gatewayRetriever =
                () -> CompletableFuture.completedFuture(restfulGateway);
        final Duration timeout = Duration.ofSeconds(10);
        final Map<String, String> responseHeaders = Collections.emptyMap();

        handler =
                new JarRunApplicationHandler(
                        gatewayRetriever,
                        timeout,
                        responseHeaders,
                        JarRunApplicationHeaders.getInstance(),
                        jarDir,
                        new Configuration());
    }

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
                        .setSubmitApplicationFunction(
                                application -> {
                                    if (application instanceof PackagedProgramApplication) {
                                        LAST_SUBMITTED_APPLICATION_REFERENCE.set(
                                                (PackagedProgramApplication) application);
                                        return CompletableFuture.completedFuture(Acknowledge.get());
                                    }
                                    return FutureUtils.completedExceptionally(
                                            new FlinkRuntimeException(
                                                    "Unsupported application type"));
                                })
                        .build();

        gatewayRetriever = () -> CompletableFuture.completedFuture(restfulGateway);
        localAddressFuture = CompletableFuture.completedFuture("shazam://localhost:12345");
        timeout = Duration.ofSeconds(10);
        responseHeaders = Collections.emptyMap();
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

        validateDefaultApplication();
    }

    @Test
    void testExceptionThrownWithoutManifestOrEntryClass() throws Exception {
        final HandlerRequest<JarRunApplicationRequestBody> request =
                createRequest(
                        getDefaultJarRequestBody(),
                        getUnresolvedJarMessageParameters(),
                        getUnresolvedJarMessageParameters(),
                        jarWithoutManifest);

        assertThatThrownBy(() -> handler.handleRequest(request, restfulGateway).get())
                .matches(
                        e -> {
                            final Throwable throwable =
                                    ExceptionUtils.stripCompletionException(e.getCause());

                            final Optional<ProgramInvocationException> invocationException =
                                    ExceptionUtils.findThrowable(
                                            throwable, ProgramInvocationException.class);

                            assertThat(invocationException).isPresent();

                            final String exceptionMsg = invocationException.get().getMessage();
                            assertThat(exceptionMsg)
                                    .contains(
                                            "Neither a 'Main-Class', nor a 'program-class' entry was found in the jar file.");

                            return true;
                        });
    }

    @Test
    void testQueryParameters() throws Exception {
        handleRequest(
                createRequest(
                        getDefaultJarRequestBody(),
                        getJarMessageParameters(),
                        getUnresolvedJarMessageParameters(),
                        jarWithoutManifest));

        validateApplication();
    }

    @Test
    void testRequestBody() throws Exception {
        handleRequest(
                createRequest(
                        getJarRequestBody(),
                        getUnresolvedJarMessageParameters(),
                        getUnresolvedJarMessageParameters(),
                        jarWithoutManifest));

        validateApplication();
    }

    @Test
    void testConfigurationViaRequestBody() throws Exception {
        handleRequest(
                createRequest(
                        getJarRequestWithConfiguration(),
                        getUnresolvedJarMessageParameters(),
                        getUnresolvedJarMessageParameters(),
                        jarWithManifest));

        validateApplicationWithConfiguration();
    }

    @Test
    void testJobId() throws Exception {
        final JobID jobId = new JobID();
        handleRequest(
                createRequest(
                        getJarRequestBodyWithJobId(jobId),
                        getUnresolvedJarMessageParameters(),
                        getUnresolvedJarMessageParameters(),
                        jarWithManifest));

        final PackagedProgramApplication application =
                LAST_SUBMITTED_APPLICATION_REFERENCE.getAndSet(null);

        assertThat(
                        application
                                .getConfiguration()
                                .get(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID))
                .isEqualTo(jobId.toHexString());
    }

    @Test
    void testApplicationId() throws Exception {
        final ApplicationID applicationId = new ApplicationID();
        handleRequest(
                createRequest(
                        getJarRequestBodyWithApplicationId(applicationId),
                        getUnresolvedJarMessageParameters(),
                        getUnresolvedJarMessageParameters(),
                        jarWithManifest));

        final PackagedProgramApplication application =
                LAST_SUBMITTED_APPLICATION_REFERENCE.getAndSet(null);

        assertThat(application.getApplicationId()).isEqualTo(applicationId);
    }

    @Test
    void testParameterPrioritization() throws Exception {
        // parameters via query parameters and JSON request, JSON should be prioritized
        handleRequest(
                createRequest(
                        getJarRequestBody(),
                        getWrongJarMessageParameters(),
                        getUnresolvedJarMessageParameters(),
                        jarWithoutManifest));

        validateApplication();
    }

    @Test
    void testExceptionNotThrownWithEagerSink() throws Exception {
        final Path jarLocation = Paths.get(System.getProperty("targetDir"));
        final String parameterProgramWithEagerSink = "parameter-program-with-eager-sink.jar";
        Path jarWithEagerSink =
                Files.copy(
                        jarLocation.resolve(parameterProgramWithEagerSink),
                        jarDir.resolve("program-with-eager-sink.jar"));

        // the handler do not run the program and should not throw an exception
        handleRequest(
                createRequest(
                        getDefaultJarRequestBody(),
                        getUnresolvedJarMessageParameters(),
                        getUnresolvedJarMessageParameters(),
                        jarWithEagerSink));

        validateDefaultApplication();
    }

    @Test
    void testEmptySavepointPath() throws Exception {
        handleRequest(
                createRequest(
                        getJarRequestBodyWithSavepointPath(""),
                        getUnresolvedJarMessageParameters(),
                        getUnresolvedJarMessageParameters(),
                        jarWithManifest));

        final PackagedProgramApplication application =
                LAST_SUBMITTED_APPLICATION_REFERENCE.getAndSet(null);
        final SavepointRestoreSettings savepointRestoreSettings =
                SavepointRestoreSettings.fromConfiguration(application.getConfiguration());

        assertThat(savepointRestoreSettings).isEqualTo(SavepointRestoreSettings.none());
    }

    @Test
    void testParallelismOverrides() throws Exception {
        handleRequest(
                createRequest(
                        getJarRequestWithConfiguration(),
                        getUnresolvedJarMessageParameters(),
                        getUnresolvedJarMessageParameters(),
                        jarWithManifest));

        final PackagedProgramApplication application =
                LAST_SUBMITTED_APPLICATION_REFERENCE.getAndSet(null);

        assertThat(application.getConfiguration().get(PipelineOptions.PARALLELISM_OVERRIDES))
                .containsOnlyKeys("v1")
                .containsEntry("v1", "10");
    }

    protected static HandlerRequest<JarRunApplicationRequestBody> createRequest(
            JarRunApplicationRequestBody requestBody,
            JarRunApplicationMessageParameters parameters,
            JarRunApplicationMessageParameters unresolvedMessageParameters,
            Path jar)
            throws HandlerRequestException {

        final Map<String, List<String>> queryParameterAsMap =
                parameters.getQueryParameters().stream()
                        .filter(MessageParameter::isResolved)
                        .collect(
                                Collectors.toMap(
                                        MessageParameter::getKey,
                                        JarRunApplicationHandlerParameterTest::getValuesAsString));

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

    JarRunApplicationMessageParameters getUnresolvedJarMessageParameters() {
        return handler.getMessageHeaders().getUnresolvedMessageParameters();
    }

    JarRunApplicationMessageParameters getJarMessageParameters() {
        final JarRunApplicationMessageParameters parameters = getUnresolvedJarMessageParameters();
        parameters.allowNonRestoredStateQueryParameter.resolve(
                Collections.singletonList(ALLOW_NON_RESTORED_STATE_QUERY));
        parameters.savepointPathQueryParameter.resolve(Collections.singletonList(RESTORE_PATH));
        parameters.entryClassQueryParameter.resolve(
                Collections.singletonList(ParameterProgram.class.getCanonicalName()));
        parameters.parallelismQueryParameter.resolve(Collections.singletonList(PARALLELISM));
        parameters.programArgQueryParameter.resolve(Arrays.asList(PROG_ARGS));
        return parameters;
    }

    JarRunApplicationMessageParameters getWrongJarMessageParameters() {
        List<String> wrongArgs =
                Arrays.stream(PROG_ARGS).map(a -> a + "wrong").collect(Collectors.toList());
        final JarRunApplicationMessageParameters parameters = getUnresolvedJarMessageParameters();
        parameters.allowNonRestoredStateQueryParameter.resolve(Collections.singletonList(false));
        parameters.savepointPathQueryParameter.resolve(Collections.singletonList("/no/uh"));
        parameters.entryClassQueryParameter.resolve(
                Collections.singletonList("please.dont.run.me"));
        parameters.parallelismQueryParameter.resolve(Collections.singletonList(64));
        parameters.programArgQueryParameter.resolve(wrongArgs);
        return parameters;
    }

    JarRunApplicationRequestBody getDefaultJarRequestBody() {
        return new JarRunApplicationRequestBody();
    }

    JarRunApplicationRequestBody getJarRequestBody() {
        return new JarRunApplicationRequestBody(
                ParameterProgram.class.getCanonicalName(),
                Arrays.asList(PROG_ARGS),
                PARALLELISM,
                null,
                ALLOW_NON_RESTORED_STATE_QUERY,
                RESTORE_PATH,
                RESTORE_MODE,
                FLINK_CONFIGURATION.toMap(),
                null);
    }

    private JarRunApplicationRequestBody getJarRequestBodyWithSavepointPath(String savepointPath) {
        return new JarRunApplicationRequestBody(
                ParameterProgram.class.getCanonicalName(),
                Arrays.asList(PROG_ARGS),
                PARALLELISM,
                null,
                ALLOW_NON_RESTORED_STATE_QUERY,
                savepointPath,
                RESTORE_MODE,
                null,
                null);
    }

    JarRunApplicationRequestBody getJarRequestBodyWithJobId(JobID jobId) {
        return new JarRunApplicationRequestBody(
                null, null, null, jobId, null, null, null, null, null);
    }

    JarRunApplicationRequestBody getJarRequestBodyWithApplicationId(ApplicationID applicationId) {
        return new JarRunApplicationRequestBody(
                null, null, null, null, null, null, null, null, applicationId);
    }

    JarRunApplicationRequestBody getJarRequestWithConfiguration() {
        return new JarRunApplicationRequestBody(
                null, null, null, null, null, null, null, FLINK_CONFIGURATION.toMap(), null);
    }

    void handleRequest(HandlerRequest<JarRunApplicationRequestBody> request) throws Exception {
        handler.handleRequest(request, restfulGateway).get();
    }

    void validateDefaultApplication() {
        PackagedProgramApplication application =
                LAST_SUBMITTED_APPLICATION_REFERENCE.getAndSet(null);

        assertThat(application.getPackagedProgram().getArguments()).isEmpty();

        final Configuration configuration = application.getConfiguration();
        assertThat(configuration.get(CoreOptions.DEFAULT_PARALLELISM))
                .isEqualTo(CoreOptions.DEFAULT_PARALLELISM.defaultValue().intValue());

        final SavepointRestoreSettings savepointRestoreSettings =
                SavepointRestoreSettings.fromConfiguration(configuration);
        assertThat(savepointRestoreSettings.allowNonRestoredState()).isFalse();
        assertThat(savepointRestoreSettings.getRestorePath()).isNull();
    }

    void validateApplication() {
        PackagedProgramApplication application =
                LAST_SUBMITTED_APPLICATION_REFERENCE.getAndSet(null);

        assertThat(application.getPackagedProgram().getArguments()).isEqualTo(PROG_ARGS);

        final Configuration configuration = application.getConfiguration();
        assertThat(configuration.get(CoreOptions.DEFAULT_PARALLELISM)).isEqualTo(PARALLELISM);

        final SavepointRestoreSettings savepointRestoreSettings =
                SavepointRestoreSettings.fromConfiguration(configuration);
        assertThat(savepointRestoreSettings.allowNonRestoredState())
                .isEqualTo(ALLOW_NON_RESTORED_STATE_QUERY);
        assertThat(savepointRestoreSettings.getRestorePath()).isEqualTo(RESTORE_PATH);
    }

    void validateApplicationWithConfiguration() {
        PackagedProgramApplication application =
                LAST_SUBMITTED_APPLICATION_REFERENCE.getAndSet(null);
        final Configuration configuration = application.getConfiguration();

        assertThat(configuration.get(CoreOptions.DEFAULT_PARALLELISM))
                .isEqualTo(FLINK_CONFIGURATION.get(CoreOptions.DEFAULT_PARALLELISM));
        assertThat(configuration.get(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT))
                .isEqualTo(FLINK_CONFIGURATION.get(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT));

        final SavepointRestoreSettings savepointRestoreSettings =
                SavepointRestoreSettings.fromConfiguration(configuration);
        assertThat(savepointRestoreSettings.getRecoveryClaimMode())
                .isEqualTo(FLINK_CONFIGURATION.get(StateRecoveryOptions.RESTORE_MODE));
        assertThat(savepointRestoreSettings.getRestorePath())
                .isEqualTo(FLINK_CONFIGURATION.get(StateRecoveryOptions.SAVEPOINT_PATH));
        assertThat(savepointRestoreSettings.allowNonRestoredState())
                .isEqualTo(
                        FLINK_CONFIGURATION.get(
                                StateRecoveryOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE));
    }
}
