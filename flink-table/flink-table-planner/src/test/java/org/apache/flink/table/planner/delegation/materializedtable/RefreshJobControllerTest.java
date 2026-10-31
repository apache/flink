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

package org.apache.flink.table.planner.delegation.materializedtable;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobRetrievalSupportedPipelineExecutor;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.refresh.ContinuousRefreshHandler;

import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RefreshJobController}. */
class RefreshJobControllerTest {

    private final JobID jobId = new JobID();
    private final TableConfig tableConfig = TableConfig.getDefault();

    @Test
    void interruptedWaitRestoresTheInterruptFlag() {
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(
                            () ->
                                    controllerReaching(
                                                    new TestingJobClient(
                                                            jobId, new CompletableFuture<>()))
                                            .getJobStatus(handler()))
                    .isInstanceOf(TableException.class)
                    .hasCauseInstanceOf(InterruptedException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void stopThatWasNeverSentIsNotReportedAsPending() {
        final RuntimeException unreachable =
                new RuntimeException("Couldn't retrieve standalone cluster");
        final JobClient jobClient =
                new TestingJobClient(jobId, JobStatus.RUNNING)
                        .withStopRequest(
                                savepointDirectory -> {
                                    throw unreachable;
                                });

        assertThatThrownBy(
                        () ->
                                controllerReaching(jobClient)
                                        .stopWithSavepoint(handler(), "file:///savepoints/mt"))
                .isInstanceOf(TableException.class)
                .hasCauseReference(unreachable)
                .hasMessageContaining("'rest.address'")
                .hasMessageNotContaining("may still complete")
                .hasMessageNotContaining("'execution.state-recovery.path'");
    }

    @Test
    void retrievedJobClientIsReusedWithinOneOperationAndClosedAtItsEnd() {
        final CloseableTestingJobClient jobClient =
                new CloseableTestingJobClient(jobId, JobStatus.RUNNING);
        final CountingRetriever retriever = new CountingRetriever(jobClient);
        final RefreshJobController controller = controller(retriever);

        controller.getJobStatus(handler());
        controller.cancel(handler());
        controller.getJobStatus(handler());

        assertThat(retriever.retrievals).isEqualTo(1);
        assertThat(jobClient.isCancelRequested()).isTrue();
        assertThat(jobClient.closed).isFalse();

        controller.releaseRetrievedJobClients();

        assertThat(jobClient.closed).isTrue();
        controller.getJobStatus(handler());
        assertThat(retriever.retrievals).isEqualTo(2);
    }

    private RefreshJobController controller(CountingRetriever retriever) {
        return new RefreshJobController(
                tableConfig, getClass().getClassLoader(), configuration -> Optional.of(retriever));
    }

    private RefreshJobController controllerReaching(JobClient jobClient) {
        return controller(new CountingRetriever(jobClient));
    }

    private ContinuousRefreshHandler handler() {
        return new ContinuousRefreshHandler(
                "remote", StandaloneClusterId.getInstance().toString(), jobId.toHexString());
    }

    private static final class CountingRetriever implements JobRetrievalSupportedPipelineExecutor {

        private final JobClient jobClient;
        private int retrievals;

        private CountingRetriever(JobClient jobClient) {
            this.jobClient = jobClient;
        }

        @Override
        public CompletableFuture<JobClient> execute(
                Pipeline pipeline, Configuration configuration, ClassLoader userCodeClassloader) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<JobClient> retrieveJobClient(
                JobID jobId, Configuration configuration, ClassLoader userCodeClassloader) {
            retrievals++;
            return CompletableFuture.completedFuture(jobClient);
        }
    }

    private static final class CloseableTestingJobClient extends TestingJobClient
            implements AutoCloseable {

        private boolean closed;

        private CloseableTestingJobClient(JobID jobId, JobStatus jobStatus) {
            super(jobId, jobStatus);
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
