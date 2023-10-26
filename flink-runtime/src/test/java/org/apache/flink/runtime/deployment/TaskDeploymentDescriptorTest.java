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

package org.apache.flink.runtime.deployment;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.BatchTask;
import org.apache.flink.runtime.util.DefaultGroupCache;
import org.apache.flink.runtime.util.GroupCache;
import org.apache.flink.runtime.util.NoOpGroupCache;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.types.Either;
import org.apache.flink.util.SerializedValue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link TaskDeploymentDescriptor}. */
class TaskDeploymentDescriptorTest {

    @TempDir Path temporaryFolder;

    private static final JobID jobID = new JobID();
    private static final JobVertexID vertexID = new JobVertexID();
    private static final ExecutionAttemptID execId = createExecutionAttemptId(vertexID);
    private static final AllocationID allocationId = new AllocationID();
    private static final String jobName = "job name";
    private static final String taskName = "task name";
    private static final int numberOfKeyGroups = 1;
    private static final int currentNumberOfSubtasks = 1;
    private static final Configuration jobConfiguration = new Configuration();
    private static final Configuration taskConfiguration = new Configuration();
    private static final Class<? extends AbstractInvokable> invokableClass = BatchTask.class;
    private static final List<ResultPartitionDeploymentDescriptor> producedResults =
            new ArrayList<ResultPartitionDeploymentDescriptor>(0);
    private static final List<InputGateDeploymentDescriptor> inputGates =
            new ArrayList<InputGateDeploymentDescriptor>(0);
    private static final List<PermanentBlobKey> requiredJars = new ArrayList<>(0);
    private static final List<URL> requiredClasspaths = new ArrayList<>(0);
    private static final TaskStateSnapshot taskStateHandles = new TaskStateSnapshot();
    private static final JobManagerTaskRestore taskRestore =
            new JobManagerTaskRestore(1L, taskStateHandles);

    private final SerializedValue<ExecutionConfig> executionConfig =
            new SerializedValue<>(new ExecutionConfig());

    private final JobInformation jobInformation =
            new JobInformation(
                    jobID,
                    jobName,
                    executionConfig,
                    jobConfiguration,
                    requiredJars,
                    requiredClasspaths);
    private final SerializedValue<JobInformation> serializedJobInformation =
            new SerializedValue<>(jobInformation);

    private final TaskInformation taskInformation =
            new TaskInformation(
                    vertexID,
                    taskName,
                    currentNumberOfSubtasks,
                    numberOfKeyGroups,
                    invokableClass.getName(),
                    taskConfiguration);
    private final SerializedValue<TaskInformation> serializedJobVertexInformation =
            new SerializedValue<>(taskInformation);

    TaskDeploymentDescriptorTest() throws IOException {}

    @Test
    void testSerialization() throws Exception {
        final TaskDeploymentDescriptor orig =
                createTaskDeploymentDescriptor(
                        new TaskDeploymentDescriptor.NonOffloaded<>(serializedJobInformation),
                        new TaskDeploymentDescriptor.NonOffloaded<>(
                                serializedJobVertexInformation));

        final TaskDeploymentDescriptor copy = CommonTestUtils.createCopySerializable(orig);

        assertThat(orig.getJobInformation()).isNotSameAs(copy.getJobInformation());
        assertThat(orig.getTaskInformation()).isNotSameAs(copy.getTaskInformation());
        assertThat(orig.getExecutionAttemptId()).isNotSameAs(copy.getExecutionAttemptId());
        assertThat(orig.getTaskRestore()).isNotSameAs(copy.getTaskRestore());
        assertThat(orig.getProducedPartitions()).isNotSameAs(copy.getProducedPartitions());
        assertThat(orig.getInputGates()).isNotSameAs(copy.getInputGates());

        assertThat(orig.getJobInformation()).isEqualTo(copy.getJobInformation());
        assertThat(orig.getTaskInformation()).isEqualTo(copy.getTaskInformation());
        assertThat(orig.getExecutionAttemptId()).isEqualTo(copy.getExecutionAttemptId());
        assertThat(orig.getAllocationId()).isEqualTo(copy.getAllocationId());
        assertThat(orig.getSubtaskIndex()).isEqualTo(copy.getSubtaskIndex());
        assertThat(orig.getAttemptNumber()).isEqualTo(copy.getAttemptNumber());
        assertThat(orig.getTaskRestore().getRestoreCheckpointId())
                .isEqualTo(copy.getTaskRestore().getRestoreCheckpointId());
        assertThat(orig.getTaskRestore().getTaskStateSnapshot())
                .isEqualTo(copy.getTaskRestore().getTaskStateSnapshot());
        assertThat(orig.getProducedPartitions()).isEqualTo(copy.getProducedPartitions());
        assertThat(orig.getInputGates()).isEqualTo(copy.getInputGates());
    }

    @Test
    void testOffLoadedAndNonOffLoadedPayload() throws IOException, ClassNotFoundException {
        final TaskDeploymentDescriptor taskDeploymentDescriptor =
                createTaskDeploymentDescriptor(
                        new TaskDeploymentDescriptor.NonOffloaded<>(serializedJobInformation),
                        new TaskDeploymentDescriptor.Offloaded<>(new PermanentBlobKey()));

        JobInformation actualJobInformation = taskDeploymentDescriptor.getJobInformation();
        assertThat(actualJobInformation).isEqualTo(jobInformation);

        assertThatThrownBy(taskDeploymentDescriptor::getTaskInformation)
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testTaskInformationCache() throws IOException, ClassNotFoundException {
        try (BlobServer blobServer = setupBlobServer()) {
            // Serialize taskInformation to blobServer and get the permanentBlobKey
            Either<SerializedValue<TaskInformation>, PermanentBlobKey> taskInformationOrBlobKey =
                    BlobWriter.serializeAndTryOffload(taskInformation, jobID, blobServer);
            assertThat(taskInformationOrBlobKey.isRight()).isTrue();
            PermanentBlobKey permanentBlobKey = taskInformationOrBlobKey.right();

            GroupCache<JobID, PermanentBlobKey, TaskInformation> taskInformationCache =
                    new DefaultGroupCache.Factory<JobID, PermanentBlobKey, TaskInformation>()
                            .create();
            // Test for tdd1
            final TaskDeploymentDescriptor tdd1 =
                    createTaskDeploymentDescriptor(
                            new TaskDeploymentDescriptor.NonOffloaded<>(serializedJobInformation),
                            new TaskDeploymentDescriptor.Offloaded<>(permanentBlobKey));
            assertThat(taskInformationCache.get(jobID, permanentBlobKey)).isNull();
            tdd1.loadBigData(
                    blobServer,
                    new NoOpGroupCache<>(),
                    taskInformationCache,
                    new NoOpGroupCache<>());
            TaskInformation taskInformation1 = tdd1.getTaskInformation();
            assertThat(taskInformation1).isEqualTo(taskInformation);
            // The TaskInformation is cached in taskInformationCache, and it's equals to
            // taskInformation1.
            assertThat(taskInformationCache.get(jobID, permanentBlobKey))
                    .isNotNull()
                    .isEqualTo(taskInformation1);

            // Test for tdd2
            final TaskDeploymentDescriptor tdd2 =
                    createTaskDeploymentDescriptor(
                            new TaskDeploymentDescriptor.NonOffloaded<>(serializedJobInformation),
                            new TaskDeploymentDescriptor.Offloaded<>(permanentBlobKey));
            tdd2.loadBigData(
                    blobServer,
                    new NoOpGroupCache<>(),
                    taskInformationCache,
                    new NoOpGroupCache<>());
            TaskInformation taskInformation2 = tdd2.getTaskInformation();
            // The TaskInformation2 is equals to taskInformation1 and original taskInformation, but
            // they are not same.
            assertThat(taskInformation2)
                    .isNotNull()
                    .isEqualTo(taskInformation1)
                    .isNotSameAs(taskInformation1)
                    .isEqualTo(taskInformation);
            // Configuration may be changed by subtask, so the configuration must be not same.
            assertThat(taskInformation2.getTaskConfiguration())
                    .isNotNull()
                    .isEqualTo(taskInformation1.getTaskConfiguration())
                    .isNotSameAs(taskInformation1.getTaskConfiguration());
        }
    }

    private BlobServer setupBlobServer() throws IOException {

        Configuration config = new Configuration();
        // always offload the serialized job and task information
        config.setInteger(BlobServerOptions.OFFLOAD_MINSIZE, 0);
        BlobServer blobServer =
                new BlobServer(
                        config, TempDirUtils.newFolder(temporaryFolder), new VoidBlobStore());
        blobServer.start();
        return blobServer;
    }

    @Nonnull
    private TaskDeploymentDescriptor createTaskDeploymentDescriptor(
            TaskDeploymentDescriptor.MaybeOffloaded<JobInformation> jobInformation,
            TaskDeploymentDescriptor.MaybeOffloaded<TaskInformation> taskInformation) {
        return new TaskDeploymentDescriptor(
                jobID,
                jobInformation,
                taskInformation,
                execId,
                allocationId,
                taskRestore,
                producedResults,
                inputGates);
    }
}
