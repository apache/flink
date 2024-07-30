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

package org.apache.flink.table.gateway.workflow;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.util.RestClientException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.factories.WorkflowSchedulerFactoryUtil;
import org.apache.flink.table.gateway.rest.RestAPIITCaseBase;
import org.apache.flink.table.gateway.rest.handler.AbstractSqlGatewayRestHandler;
import org.apache.flink.table.gateway.rest.header.materializedtable.scheduler.CreateEmbeddedSchedulerWorkflowHeaders;
import org.apache.flink.table.gateway.rest.header.materializedtable.scheduler.DeleteEmbeddedSchedulerWorkflowHeaders;
import org.apache.flink.table.gateway.rest.header.materializedtable.scheduler.ResumeEmbeddedSchedulerWorkflowHeaders;
import org.apache.flink.table.gateway.rest.header.materializedtable.scheduler.SuspendEmbeddedSchedulerWorkflowHeaders;
import org.apache.flink.table.gateway.rest.message.materializedtable.scheduler.CreateEmbeddedSchedulerWorkflowRequestBody;
import org.apache.flink.table.gateway.rest.message.materializedtable.scheduler.CreateEmbeddedSchedulerWorkflowResponseBody;
import org.apache.flink.table.gateway.rest.message.materializedtable.scheduler.EmbeddedSchedulerWorkflowRequestBody;
import org.apache.flink.table.gateway.rest.message.materializedtable.scheduler.ResumeEmbeddedSchedulerWorkflowRequestBody;
import org.apache.flink.table.workflow.CreatePeriodicRefreshWorkflow;
import org.apache.flink.table.workflow.CreateRefreshWorkflow;
import org.apache.flink.table.workflow.DeleteRefreshWorkflow;
import org.apache.flink.table.workflow.ModifyRefreshWorkflow;
import org.apache.flink.table.workflow.ResumeRefreshWorkflow;
import org.apache.flink.table.workflow.SuspendRefreshWorkflow;
import org.apache.flink.table.workflow.WorkflowException;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.apache.flink.table.factories.FactoryUtil.WORKFLOW_SCHEDULER_TYPE;
import static org.apache.flink.table.gateway.workflow.scheduler.QuartzSchedulerUtils.QUARTZ_JOB_GROUP;
import static org.apache.flink.table.gateway.workflow.scheduler.QuartzSchedulerUtils.QUARTZ_JOB_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test basic logic of handlers inherited from {@link AbstractSqlGatewayRestHandler} in embedded
 * scheduler cases.
 */
public class EmbeddedSchedulerRelatedITCase extends RestAPIITCaseBase {

    private static final EmbeddedSchedulerWorkflowRequestBody nonExistsWorkflow =
            new EmbeddedSchedulerWorkflowRequestBody("non-exists", QUARTZ_JOB_GROUP);

    private static final ObjectIdentifier materializedTableIdentifier =
            ObjectIdentifier.of("cat", "db", "t1");
    private static final String descriptionStatement =
            String.format(
                    "ALTER MATERIALIZED TABLE %s REFRESH",
                    materializedTableIdentifier.asSerializableString());
    private static final String cronExpression = "0 0/1 * * * ?";

    private static final EmbeddedRefreshHandler nonExistsHandler =
            new EmbeddedRefreshHandler("non-exits", QUARTZ_JOB_GROUP);

    private CreateEmbeddedSchedulerWorkflowRequestBody createRequestBody;
    private CreatePeriodicRefreshWorkflow createPeriodicWorkflow;

    private EmbeddedWorkflowScheduler embeddedWorkflowScheduler;

    @BeforeEach
    void setup() throws Exception {
        String gatewayRestEndpointURL = String.format("http://%s:%s", targetAddress, port);
        createRequestBody =
                new CreateEmbeddedSchedulerWorkflowRequestBody(
                        materializedTableIdentifier.asSerializableString(),
                        cronExpression,
                        null,
                        null,
                        gatewayRestEndpointURL);
        createPeriodicWorkflow =
                new CreatePeriodicRefreshWorkflow(
                        materializedTableIdentifier,
                        descriptionStatement,
                        cronExpression,
                        null,
                        null,
                        gatewayRestEndpointURL);

        Configuration configuration = new Configuration();
        configuration.set(WORKFLOW_SCHEDULER_TYPE, "embedded");
        configuration.setString("sql-gateway.endpoint.rest.address", targetAddress);
        configuration.setString("sql-gateway.endpoint.rest.port", String.valueOf(port));

        embeddedWorkflowScheduler =
                (EmbeddedWorkflowScheduler)
                        WorkflowSchedulerFactoryUtil.createWorkflowScheduler(
                                configuration,
                                EmbeddedSchedulerRelatedITCase.class.getClassLoader());
        embeddedWorkflowScheduler.open();
    }

    @AfterEach
    void cleanup() throws Exception {
        embeddedWorkflowScheduler.close();
    }

    @Test
    void testCreateWorkflow() throws Exception {
        CreateEmbeddedSchedulerWorkflowResponseBody createResponse =
                sendRequest(
                                CreateEmbeddedSchedulerWorkflowHeaders.getInstance(),
                                EmptyMessageParameters.getInstance(),
                                createRequestBody)
                        .get(5, TimeUnit.SECONDS);

        assertThat(createResponse.getWorkflowName())
                .isEqualTo(
                        QUARTZ_JOB_PREFIX
                                + "_"
                                + materializedTableIdentifier.asSerializableString());
        assertThat(createResponse.getWorkflowGroup()).isEqualTo(QUARTZ_JOB_GROUP);

        // create the workflow repeatedly
        CompletableFuture<CreateEmbeddedSchedulerWorkflowResponseBody> repeatedCreateFuture =
                sendRequest(
                        CreateEmbeddedSchedulerWorkflowHeaders.getInstance(),
                        EmptyMessageParameters.getInstance(),
                        createRequestBody);

        assertThatFuture(repeatedCreateFuture)
                .failsWithin(5, TimeUnit.SECONDS)
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(RestClientException.class)
                .withMessageContaining(
                        "Materialized table `cat`.`db`.`t1` quartz schedule job already exist, job info: default_group.quartz_job_`cat`.`db`.`t1`.")
                .satisfies(
                        e ->
                                assertThat(
                                                ((RestClientException) e.getCause())
                                                        .getHttpResponseStatus())
                                        .isEqualTo(HttpResponseStatus.INTERNAL_SERVER_ERROR));

        // delete the schedule job
        EmbeddedSchedulerWorkflowRequestBody deleteRequestBody =
                new EmbeddedSchedulerWorkflowRequestBody(
                        createResponse.getWorkflowName(), createResponse.getWorkflowGroup());
        CompletableFuture<EmptyResponseBody> deleteFuture =
                sendRequest(
                        DeleteEmbeddedSchedulerWorkflowHeaders.getInstance(),
                        EmptyMessageParameters.getInstance(),
                        deleteRequestBody);
        assertThatFuture(deleteFuture).succeedsWithin(5, TimeUnit.SECONDS);
    }

    @Test
    void testSuspendNonExistsWorkflow() throws Exception {
        CompletableFuture<EmptyResponseBody> suspendFuture =
                sendRequest(
                        SuspendEmbeddedSchedulerWorkflowHeaders.getInstance(),
                        EmptyMessageParameters.getInstance(),
                        nonExistsWorkflow);

        assertThatFuture(suspendFuture)
                .failsWithin(5, TimeUnit.SECONDS)
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(RestClientException.class)
                .withMessageContaining(
                        "Failed to suspend a non-existent quartz schedule job: default_group.non-exists")
                .satisfies(
                        e ->
                                assertThat(
                                                ((RestClientException) e.getCause())
                                                        .getHttpResponseStatus())
                                        .isEqualTo(HttpResponseStatus.INTERNAL_SERVER_ERROR));
    }

    @Test
    void testResumeNonExistsWorkflow() throws Exception {
        ResumeEmbeddedSchedulerWorkflowRequestBody resumeRequestBody =
                new ResumeEmbeddedSchedulerWorkflowRequestBody(
                        nonExistsWorkflow.getWorkflowName(),
                        nonExistsWorkflow.getWorkflowGroup(),
                        null);
        CompletableFuture<EmptyResponseBody> suspendFuture =
                sendRequest(
                        ResumeEmbeddedSchedulerWorkflowHeaders.getInstance(),
                        EmptyMessageParameters.getInstance(),
                        resumeRequestBody);

        assertThatFuture(suspendFuture)
                .failsWithin(5, TimeUnit.SECONDS)
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(RestClientException.class)
                .withMessageContaining(
                        "Failed to resume a non-existent quartz schedule job: default_group.non-exists")
                .satisfies(
                        e ->
                                assertThat(
                                                ((RestClientException) e.getCause())
                                                        .getHttpResponseStatus())
                                        .isEqualTo(HttpResponseStatus.INTERNAL_SERVER_ERROR));
    }

    @Test
    void testDeleteNonExistsWorkflow() throws Exception {
        CompletableFuture<EmptyResponseBody> suspendFuture =
                sendRequest(
                        DeleteEmbeddedSchedulerWorkflowHeaders.getInstance(),
                        EmptyMessageParameters.getInstance(),
                        nonExistsWorkflow);

        assertThatFuture(suspendFuture).succeedsWithin(5, TimeUnit.SECONDS);
    }

    @Test
    void testCreateWorkflowByWorkflowSchedulerInterface() throws Exception {
        // create workflow
        EmbeddedRefreshHandler actual =
                embeddedWorkflowScheduler.createRefreshWorkflow(createPeriodicWorkflow);

        EmbeddedRefreshHandler expected =
                new EmbeddedRefreshHandler(
                        QUARTZ_JOB_PREFIX
                                + "_"
                                + materializedTableIdentifier.asSerializableString(),
                        QUARTZ_JOB_GROUP);
        assertThat(actual).isEqualTo(expected);

        // create workflow repeatedly
        assertThatThrownBy(
                        () ->
                                embeddedWorkflowScheduler.createRefreshWorkflow(
                                        createPeriodicWorkflow))
                .isInstanceOf(WorkflowException.class)
                .hasMessage(
                        "Failed to create periodic refresh workflow for materialized table `cat`.`db`.`t1`.");

        // suspend, just to verify suspend function can work
        SuspendRefreshWorkflow<EmbeddedRefreshHandler> suspendRefreshWorkflow =
                new SuspendRefreshWorkflow<>(actual);
        embeddedWorkflowScheduler.modifyRefreshWorkflow(suspendRefreshWorkflow);

        // resume, just to verify suspend function can work
        ResumeRefreshWorkflow<EmbeddedRefreshHandler> resumeRefreshWorkflow =
                new ResumeRefreshWorkflow<>(actual, Collections.emptyMap());
        embeddedWorkflowScheduler.modifyRefreshWorkflow(resumeRefreshWorkflow);

        // delete, just to verify suspend function can work
        DeleteRefreshWorkflow<EmbeddedRefreshHandler> deleteRefreshWorkflow =
                new DeleteRefreshWorkflow<>(actual);
        embeddedWorkflowScheduler.deleteRefreshWorkflow(deleteRefreshWorkflow);
    }

    @Test
    void testCreateWorkflowWithUnsupportedTypeByWorkflowSchedulerInterface() {
        assertThatThrownBy(
                        () ->
                                embeddedWorkflowScheduler.createRefreshWorkflow(
                                        new UnsupportedCreateRefreshWorkflow()))
                .isInstanceOf(WorkflowException.class)
                .hasMessage(
                        "Unsupported create refresh workflow type UnsupportedCreateRefreshWorkflow.");
    }

    @Test
    void testModifyWorkflowWithUnsupportedTypeByWorkflowSchedulerInterface() {
        assertThatThrownBy(
                        () ->
                                embeddedWorkflowScheduler.modifyRefreshWorkflow(
                                        new UnsupportedModifyRefreshWorkflow()))
                .isInstanceOf(WorkflowException.class)
                .hasMessage(
                        "Unsupported modify refresh workflow type UnsupportedModifyRefreshWorkflow.");
    }

    @Test
    void testNonExistsWorkflowByWorkflowSchedulerInterface() throws WorkflowException {
        // suspend case
        assertThatThrownBy(
                        () ->
                                embeddedWorkflowScheduler.modifyRefreshWorkflow(
                                        new SuspendRefreshWorkflow<>(nonExistsHandler)))
                .isInstanceOf(WorkflowException.class)
                .hasMessage(
                        "Failed to suspend refresh workflow {\n"
                                + "  workflowName: non-exits,\n"
                                + "  workflowGroup: default_group\n"
                                + "}.");

        // resume case
        assertThatThrownBy(
                        () ->
                                embeddedWorkflowScheduler.modifyRefreshWorkflow(
                                        new ResumeRefreshWorkflow<>(
                                                nonExistsHandler, Collections.emptyMap())))
                .isInstanceOf(WorkflowException.class)
                .hasMessage(
                        "Failed to resume refresh workflow {\n"
                                + "  workflowName: non-exits,\n"
                                + "  workflowGroup: default_group\n"
                                + "}.");

        // delete case
        embeddedWorkflowScheduler.deleteRefreshWorkflow(
                new DeleteRefreshWorkflow<>(nonExistsHandler));
    }

    /** Just used for test. */
    private static class UnsupportedCreateRefreshWorkflow implements CreateRefreshWorkflow {}

    /** Just used for test. */
    private static class UnsupportedModifyRefreshWorkflow
            implements ModifyRefreshWorkflow<EmbeddedRefreshHandler> {

        @Override
        public EmbeddedRefreshHandler getRefreshHandler() {
            return new EmbeddedRefreshHandler("a", "b");
        }
    }
}
