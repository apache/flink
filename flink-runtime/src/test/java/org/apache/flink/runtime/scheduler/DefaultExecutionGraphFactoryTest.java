/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.FlinkMatchers;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.StandaloneCompletedCheckpointStore;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory;
import org.apache.flink.runtime.executiongraph.DefaultVertexAttemptNumberStore;
import org.apache.flink.runtime.io.network.partition.NoOpJobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobmaster.DefaultExecutionDeploymentTracker;
import org.apache.flink.runtime.jobmaster.TestUtils;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.shuffle.ShuffleTestUtils;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorResource;
import org.apache.flink.util.TestLogger;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;

import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/** Tests for the {@link DefaultExecutionGraphFactory}. */
public class DefaultExecutionGraphFactoryTest extends TestLogger {

    @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    @ClassRule
    public static final TestExecutorResource<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorResource();

    @Test
    public void testRestoringModifiedJobFromSavepointFails() throws Exception {
        final JobGraph jobGraphWithNewOperator = createJobGraphWithSavepoint(false, 42L);

        final ExecutionGraphFactory executionGraphFactory = createExecutionGraphFactory();

        try {
            executionGraphFactory.createAndRestoreExecutionGraph(
                    jobGraphWithNewOperator,
                    new StandaloneCompletedCheckpointStore(1),
                    new CheckpointsCleaner(),
                    new StandaloneCheckpointIDCounter(),
                    TaskDeploymentDescriptorFactory.PartitionLocationConstraint.CAN_BE_UNKNOWN,
                    0L,
                    new DefaultVertexAttemptNumberStore(),
                    SchedulerBase.computeVertexParallelismStore(jobGraphWithNewOperator),
                    (execution, previousState, newState) -> {},
                    rp -> false,
                    log);
            fail("Expected ExecutionGraph creation to fail because of non restored state.");
        } catch (Exception e) {
            assertThat(
                    e, FlinkMatchers.containsMessage("Failed to rollback to checkpoint/savepoint"));
        }
    }

    @Test
    public void testRestoringModifiedJobFromSavepointWithAllowNonRestoredStateSucceeds()
            throws Exception {
        // create savepoint data
        final long savepointId = 42L;
        final JobGraph jobGraphWithNewOperator = createJobGraphWithSavepoint(true, savepointId);

        final ExecutionGraphFactory executionGraphFactory = createExecutionGraphFactory();

        final StandaloneCompletedCheckpointStore completedCheckpointStore =
                new StandaloneCompletedCheckpointStore(1);
        executionGraphFactory.createAndRestoreExecutionGraph(
                jobGraphWithNewOperator,
                completedCheckpointStore,
                new CheckpointsCleaner(),
                new StandaloneCheckpointIDCounter(),
                TaskDeploymentDescriptorFactory.PartitionLocationConstraint.CAN_BE_UNKNOWN,
                0L,
                new DefaultVertexAttemptNumberStore(),
                SchedulerBase.computeVertexParallelismStore(jobGraphWithNewOperator),
                (execution, previousState, newState) -> {},
                rp -> false,
                log);

        final CompletedCheckpoint savepoint = completedCheckpointStore.getLatestCheckpoint();

        MatcherAssert.assertThat(savepoint, notNullValue());

        MatcherAssert.assertThat(savepoint.getCheckpointID(), Matchers.is(savepointId));
    }

    @Nonnull
    private ExecutionGraphFactory createExecutionGraphFactory() {
        final ExecutionGraphFactory executionGraphFactory =
                new DefaultExecutionGraphFactory(
                        new Configuration(),
                        ClassLoader.getSystemClassLoader(),
                        new DefaultExecutionDeploymentTracker(),
                        EXECUTOR_RESOURCE.getExecutor(),
                        EXECUTOR_RESOURCE.getExecutor(),
                        Time.milliseconds(0L),
                        UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup(),
                        VoidBlobWriter.getInstance(),
                        ShuffleTestUtils.DEFAULT_SHUFFLE_MASTER,
                        NoOpJobMasterPartitionTracker.INSTANCE);
        return executionGraphFactory;
    }

    @Nonnull
    private JobGraph createJobGraphWithSavepoint(boolean allowNonRestoredState, long savepointId)
            throws IOException {
        // create savepoint data
        final OperatorID operatorID = new OperatorID();
        final File savepointFile =
                TestUtils.createSavepointWithOperatorState(
                        TEMPORARY_FOLDER.newFile(), savepointId, operatorID);

        // set savepoint settings which don't allow non restored state
        final SavepointRestoreSettings savepointRestoreSettings =
                SavepointRestoreSettings.forPath(
                        savepointFile.getAbsolutePath(), allowNonRestoredState);

        // create a new operator
        final JobVertex jobVertex = new JobVertex("New operator");
        jobVertex.setInvokableClass(NoOpInvokable.class);
        jobVertex.setParallelism(1);

        // this test will fail in the end due to the previously created Savepoint having a state for
        // a given OperatorID that does not match any operator of the newly created JobGraph
        return TestUtils.createJobGraphFromJobVerticesWithCheckpointing(
                savepointRestoreSettings, jobVertex);
    }
}
