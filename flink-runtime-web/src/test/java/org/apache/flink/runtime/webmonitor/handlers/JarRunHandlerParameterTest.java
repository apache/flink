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
import org.apache.flink.client.deployment.application.DetachedApplicationRunner;
import org.apache.flink.client.deployment.application.executors.EmbeddedExecutor;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.runtime.webmonitor.testutils.ParameterProgram;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

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
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the parameter handling of the {@link JarRunHandler}. */
class JarRunHandlerParameterTest
        extends JarHandlerParameterTest<JarRunRequestBody, JarRunMessageParameters> {
    private static final boolean ALLOW_NON_RESTORED_STATE_QUERY = true;
    private static final String RESTORE_PATH = "/foo/bar";
    private static final RestoreMode RESTORE_MODE = RestoreMode.CLAIM;

    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_EXTENSION =
            TestingUtils.defaultExecutorExtension();

    private static JarRunHandler handler;

    private static Path jarWithEagerSink;

    private static final Configuration FLINK_CONFIGURATION =
            new Configuration()
                    .set(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT, 120000L)
                    .set(CoreOptions.DEFAULT_PARALLELISM, 57)
                    .set(SavepointConfigOptions.SAVEPOINT_PATH, "/foo/bar/test")
                    .set(SavepointConfigOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE, false)
                    .set(SavepointConfigOptions.RESTORE_MODE, RESTORE_MODE);

    @BeforeAll
    static void setup(@TempDir File tempDir) throws Exception {
        init(tempDir);
        final GatewayRetriever<TestingDispatcherGateway> gatewayRetriever =
                () -> CompletableFuture.completedFuture(restfulGateway);
        final Time timeout = Time.seconds(10);
        final Map<String, String> responseHeaders = Collections.emptyMap();

        final Path jarLocation = Paths.get(System.getProperty("targetDir"));
        final String parameterProgramWithEagerSink = "parameter-program-with-eager-sink.jar";
        jarWithEagerSink =
                Files.copy(
                        jarLocation.resolve(parameterProgramWithEagerSink),
                        jarDir.resolve("program-with-eager-sink.jar"));

        handler =
                new JarRunHandler(
                        gatewayRetriever,
                        timeout,
                        responseHeaders,
                        JarRunHeaders.getInstance(),
                        jarDir,
                        new Configuration(),
                        EXECUTOR_EXTENSION.getExecutor(),
                        ConfigurationVerifyingDetachedApplicationRunner::new);
    }

    private static class ConfigurationVerifyingDetachedApplicationRunner
            extends DetachedApplicationRunner {

        public ConfigurationVerifyingDetachedApplicationRunner() {
            super(true);
        }

        @Override
        public List<JobID> run(
                DispatcherGateway dispatcherGateway,
                PackagedProgram program,
                Configuration configuration) {
            assertThat(configuration.get(DeploymentOptions.ATTACHED)).isFalse();
            assertThat(configuration.get(DeploymentOptions.TARGET))
                    .isEqualTo(EmbeddedExecutor.NAME);
            return super.run(dispatcherGateway, program, configuration);
        }
    }

    @Override
    JarRunMessageParameters getUnresolvedJarMessageParameters() {
        return handler.getMessageHeaders().getUnresolvedMessageParameters();
    }

    @Override
    JarRunMessageParameters getJarMessageParameters(ProgramArgsParType programArgsParType) {
        final JarRunMessageParameters parameters = getUnresolvedJarMessageParameters();
        parameters.allowNonRestoredStateQueryParameter.resolve(
                Collections.singletonList(ALLOW_NON_RESTORED_STATE_QUERY));
        parameters.savepointPathQueryParameter.resolve(Collections.singletonList(RESTORE_PATH));
        parameters.entryClassQueryParameter.resolve(
                Collections.singletonList(ParameterProgram.class.getCanonicalName()));
        parameters.parallelismQueryParameter.resolve(Collections.singletonList(PARALLELISM));
        if (programArgsParType == ProgramArgsParType.String
                || programArgsParType == ProgramArgsParType.Both) {
            parameters.programArgsQueryParameter.resolve(
                    Collections.singletonList(String.join(" ", PROG_ARGS)));
        }
        if (programArgsParType == ProgramArgsParType.List
                || programArgsParType == ProgramArgsParType.Both) {
            parameters.programArgQueryParameter.resolve(Arrays.asList(PROG_ARGS));
        }
        return parameters;
    }

    @Override
    JarRunMessageParameters getWrongJarMessageParameters(ProgramArgsParType programArgsParType) {
        List<String> wrongArgs =
                Arrays.stream(PROG_ARGS).map(a -> a + "wrong").collect(Collectors.toList());
        String argsWrongStr = String.join(" ", wrongArgs);
        final JarRunMessageParameters parameters = getUnresolvedJarMessageParameters();
        parameters.allowNonRestoredStateQueryParameter.resolve(Collections.singletonList(false));
        parameters.savepointPathQueryParameter.resolve(Collections.singletonList("/no/uh"));
        parameters.entryClassQueryParameter.resolve(
                Collections.singletonList("please.dont.run.me"));
        parameters.parallelismQueryParameter.resolve(Collections.singletonList(64));
        if (programArgsParType == ProgramArgsParType.String
                || programArgsParType == ProgramArgsParType.Both) {
            parameters.programArgsQueryParameter.resolve(Collections.singletonList(argsWrongStr));
        }
        if (programArgsParType == ProgramArgsParType.List
                || programArgsParType == ProgramArgsParType.Both) {
            parameters.programArgQueryParameter.resolve(wrongArgs);
        }
        return parameters;
    }

    @Override
    JarRunRequestBody getDefaultJarRequestBody() {
        return new JarRunRequestBody();
    }

    @Override
    JarRunRequestBody getJarRequestBody(ProgramArgsParType programArgsParType) {
        return new JarRunRequestBody(
                ParameterProgram.class.getCanonicalName(),
                getProgramArgsString(programArgsParType),
                getProgramArgsList(programArgsParType),
                PARALLELISM,
                null,
                ALLOW_NON_RESTORED_STATE_QUERY,
                RESTORE_PATH,
                RESTORE_MODE,
                FLINK_CONFIGURATION.toMap());
    }

    private JarRunRequestBody getJarRequestBodyWithSavepointPath(
            ProgramArgsParType programArgsParType, String savepointPath) {
        return new JarRunRequestBody(
                ParameterProgram.class.getCanonicalName(),
                getProgramArgsString(programArgsParType),
                getProgramArgsList(programArgsParType),
                PARALLELISM,
                null,
                ALLOW_NON_RESTORED_STATE_QUERY,
                savepointPath,
                RESTORE_MODE,
                null);
    }

    @Override
    JarRunRequestBody getJarRequestBodyWithJobId(JobID jobId) {
        return new JarRunRequestBody(null, null, null, null, jobId, null, null, null, null);
    }

    @Override
    JarRunRequestBody getJarRequestWithConfiguration() {
        return new JarRunRequestBody(
                null, null, null, null, null, null, null, null, FLINK_CONFIGURATION.toMap());
    }

    @Test
    void testRestHandlerExceptionThrownWithEagerSinks() throws Exception {
        final HandlerRequest<JarRunRequestBody> request =
                createRequest(
                        getDefaultJarRequestBody(),
                        getUnresolvedJarMessageParameters(),
                        getUnresolvedJarMessageParameters(),
                        jarWithEagerSink);

        assertThatThrownBy(() -> handler.handleRequest(request, restfulGateway).get())
                .matches(
                        e -> {
                            final Throwable throwable =
                                    ExceptionUtils.stripCompletionException(e.getCause());
                            assertThat(throwable).isInstanceOf(RestHandlerException.class);

                            final RestHandlerException restHandlerException =
                                    (RestHandlerException) throwable;
                            assertThat(restHandlerException.getHttpResponseStatus())
                                    .isEqualTo(HttpResponseStatus.BAD_REQUEST);

                            final Optional<ProgramInvocationException> invocationException =
                                    ExceptionUtils.findThrowable(
                                            restHandlerException, ProgramInvocationException.class);

                            assertThat(invocationException).isPresent();

                            final String exceptionMsg = invocationException.get().getMessage();
                            assertThat(exceptionMsg)
                                    .contains("Job was submitted in detached mode.");

                            return true;
                        });
    }

    @Test
    void testConfigurationWithEmptySavepointPath() throws Exception {
        final JarRunRequestBody requestBody =
                getJarRequestBodyWithSavepointPath(ProgramArgsParType.String, "");
        handleRequest(
                createRequest(
                        requestBody,
                        getUnresolvedJarMessageParameters(),
                        getUnresolvedJarMessageParameters(),
                        jarWithEagerSink));
        JobGraph jobGraph = LAST_SUBMITTED_JOB_GRAPH_REFERENCE.get();
        assertThat(jobGraph.getSavepointRestoreSettings())
                .isEqualTo(SavepointRestoreSettings.none());
    }

    @Override
    void handleRequest(HandlerRequest<JarRunRequestBody> request) throws Exception {
        handler.handleRequest(request, restfulGateway).get();
    }

    @Override
    JobGraph validateDefaultGraph() {
        JobGraph jobGraph = super.validateDefaultGraph();
        final SavepointRestoreSettings savepointRestoreSettings =
                jobGraph.getSavepointRestoreSettings();
        assertThat(savepointRestoreSettings.allowNonRestoredState()).isFalse();
        assertThat(savepointRestoreSettings.getRestorePath()).isNull();
        return jobGraph;
    }

    @Override
    JobGraph validateGraph() {
        JobGraph jobGraph = super.validateGraph();
        final SavepointRestoreSettings savepointRestoreSettings =
                jobGraph.getSavepointRestoreSettings();
        this.validateSavepointJarRunMessageParameters(savepointRestoreSettings);
        return jobGraph;
    }

    @Override
    void validateGraphWithFlinkConfig(JobGraph jobGraph) {
        final ExecutionConfig executionConfig = getExecutionConfig(jobGraph);
        assertThat(executionConfig.getParallelism())
                .isEqualTo(FLINK_CONFIGURATION.get(CoreOptions.DEFAULT_PARALLELISM));
        assertThat(executionConfig.getTaskCancellationTimeout())
                .isEqualTo(FLINK_CONFIGURATION.get(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT));

        final SavepointRestoreSettings savepointRestoreSettings =
                jobGraph.getSavepointRestoreSettings();
        assertThat(savepointRestoreSettings.getRestoreMode())
                .isEqualTo(FLINK_CONFIGURATION.get(SavepointConfigOptions.RESTORE_MODE));
        assertThat(savepointRestoreSettings.getRestorePath())
                .isEqualTo(FLINK_CONFIGURATION.get(SavepointConfigOptions.SAVEPOINT_PATH));
        assertThat(savepointRestoreSettings.allowNonRestoredState())
                .isEqualTo(
                        FLINK_CONFIGURATION.get(
                                SavepointConfigOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE));
    }

    private void validateSavepointJarRunMessageParameters(
            final SavepointRestoreSettings savepointRestoreSettings) {
        assertThat(savepointRestoreSettings.allowNonRestoredState())
                .isEqualTo(ALLOW_NON_RESTORED_STATE_QUERY);
        assertThat(savepointRestoreSettings.getRestorePath()).isEqualTo(RESTORE_PATH);
    }
}
