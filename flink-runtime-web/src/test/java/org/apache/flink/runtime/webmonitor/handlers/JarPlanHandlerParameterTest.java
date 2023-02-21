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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.jsonplan.JsonPlanGenerator;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.messages.JobPlanInfo;
import org.apache.flink.runtime.webmonitor.testutils.ParameterProgram;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the parameter handling of the {@link JarPlanHandler}. */
class JarPlanHandlerParameterTest
        extends JarHandlerParameterTest<JarPlanRequestBody, JarPlanMessageParameters> {
    private static JarPlanHandler handler;
    private static final Configuration FLINK_CONFIGURATION =
            new Configuration()
                    .set(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT, 120000L)
                    .set(CoreOptions.DEFAULT_PARALLELISM, 57);

    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_EXTENSION =
            TestingUtils.defaultExecutorExtension();

    @BeforeAll
    static void setup(@TempDir File tempDir) throws Exception {
        init(tempDir);
        handler =
                new JarPlanHandler(
                        gatewayRetriever,
                        timeout,
                        responseHeaders,
                        JarPlanGetHeaders.getInstance(),
                        jarDir,
                        new Configuration(),
                        EXECUTOR_EXTENSION.getExecutor(),
                        jobGraph -> {
                            LAST_SUBMITTED_JOB_GRAPH_REFERENCE.set(jobGraph);
                            return new JobPlanInfo(JsonPlanGenerator.generatePlan(jobGraph));
                        });
    }

    @Override
    JarPlanMessageParameters getUnresolvedJarMessageParameters() {
        return handler.getMessageHeaders().getUnresolvedMessageParameters();
    }

    @Override
    JarPlanMessageParameters getJarMessageParameters(ProgramArgsParType programArgsParType) {
        final JarPlanMessageParameters parameters = getUnresolvedJarMessageParameters();
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
    JarPlanMessageParameters getWrongJarMessageParameters(ProgramArgsParType programArgsParType) {
        List<String> wrongArgs =
                Arrays.stream(PROG_ARGS).map(a -> a + "wrong").collect(Collectors.toList());
        String argsWrongStr = String.join(" ", wrongArgs);
        final JarPlanMessageParameters parameters = getUnresolvedJarMessageParameters();
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
    JarPlanRequestBody getDefaultJarRequestBody() {
        return new JarPlanRequestBody();
    }

    @Override
    JarPlanRequestBody getJarRequestBody(ProgramArgsParType programArgsParType) {
        return new JarPlanRequestBody(
                ParameterProgram.class.getCanonicalName(),
                getProgramArgsString(programArgsParType),
                getProgramArgsList(programArgsParType),
                PARALLELISM,
                null,
                null);
    }

    @Override
    JarPlanRequestBody getJarRequestBodyWithJobId(JobID jobId) {
        return new JarPlanRequestBody(null, null, null, null, jobId, null);
    }

    @Override
    JarPlanRequestBody getJarRequestWithConfiguration() {
        return new JarPlanRequestBody(null, null, null, null, null, FLINK_CONFIGURATION.toMap());
    }

    @Override
    void handleRequest(HandlerRequest<JarPlanRequestBody> request) throws Exception {
        handler.handleRequest(request, restfulGateway).get();
    }

    @Override
    void validateGraphWithFlinkConfig(JobGraph jobGraph) {
        final ExecutionConfig executionConfig = getExecutionConfig(jobGraph);
        assertThat(executionConfig.getParallelism())
                .isEqualTo(FLINK_CONFIGURATION.get(CoreOptions.DEFAULT_PARALLELISM));
        assertThat(executionConfig.getTaskCancellationTimeout())
                .isEqualTo(FLINK_CONFIGURATION.get(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT));
    }
}
