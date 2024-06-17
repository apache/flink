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

package org.apache.flink.runtime.scheduler.adaptivebatch;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkAlignmentParams;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.mocks.MockSource;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumerator;
import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.DefaultExecutionGraph;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.executiongraph.InternalExecutionGraphAccessor;
import org.apache.flink.runtime.executiongraph.ResultPartitionBytes;
import org.apache.flink.runtime.executiongraph.TestingComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.failover.FixedDelayRestartBackoffTimeStrategy;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTrackerImpl;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmaster.event.ExecutionVertexFinishedEvent;
import org.apache.flink.runtime.jobmaster.event.FileSystemJobEventStore;
import org.apache.flink.runtime.jobmaster.event.JobEvent;
import org.apache.flink.runtime.jobmaster.event.JobEventManager;
import org.apache.flink.runtime.jobmaster.event.JobEventStore;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGateway;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.operators.coordination.EventReceivingTasks;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinatorHolder;
import org.apache.flink.runtime.operators.coordination.RecreateOnResetOperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.TestingOperatorCoordinator;
import org.apache.flink.runtime.scheduler.DefaultSchedulerBuilder;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.shuffle.DefaultShuffleMetrics;
import org.apache.flink.runtime.shuffle.JobShuffleContextImpl;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.shuffle.NettyShuffleMaster;
import org.apache.flink.runtime.shuffle.PartitionWithMetrics;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.shuffle.ShuffleMasterContextImpl;
import org.apache.flink.runtime.shuffle.ShuffleMetrics;
import org.apache.flink.runtime.source.coordinator.SourceCoordinator;
import org.apache.flink.runtime.source.coordinator.SourceCoordinatorProvider;
import org.apache.flink.runtime.source.event.ReaderRegistrationEvent;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.util.concurrent.ScheduledExecutor;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nonnull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.waitUntilExecutionVertexState;
import static org.apache.flink.runtime.scheduler.DefaultSchedulerBuilder.createCustomParallelismDecider;
import static org.apache.flink.util.Preconditions.checkState;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/** Test for batch job recovery. */
@ExtendWith(ParameterizedTestExtension.class)
public class BatchJobRecoveryTest {

    private final Duration previousWorkerRecoveryTimeout = Duration.ofSeconds(1);

    @TempDir private java.nio.file.Path temporaryFolder;

    // ---- Mocks for the underlying Operator Coordinator Context ---
    protected EventReceivingTasks receivingTasks;

    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    @RegisterExtension
    static final TestingComponentMainThreadExecutor.Extension MAIN_EXECUTOR_RESOURCE =
            new TestingComponentMainThreadExecutor.Extension();

    private final TestingComponentMainThreadExecutor mainThreadExecutor =
            MAIN_EXECUTOR_RESOURCE.getComponentMainThreadTestExecutor();

    private ScheduledExecutor delayedExecutor =
            new ScheduledExecutorServiceAdapter(EXECUTOR_RESOURCE.getExecutor());

    private static final OperatorID OPERATOR_ID = new OperatorID(1234L, 5678L);
    private static final int NUM_SPLITS = 10;
    private static final int SOURCE_PARALLELISM = 5;
    private static final int MIDDLE_PARALLELISM = 5;
    private static final int DECIDED_SINK_PARALLELISM = 2;
    private static final JobVertexID SOURCE_ID = new JobVertexID();
    private static final JobVertexID MIDDLE_ID = new JobVertexID();
    private static final JobVertexID SINK_ID = new JobVertexID();
    private static final JobID JOB_ID = new JobID();

    private SourceCoordinatorProvider<MockSourceSplit> provider;
    private FileSystemJobEventStore jobEventStore;
    private AtomicBoolean recoveryStarted;
    private List<JobEvent> persistedJobEventList;

    private byte[] serializedJobGraph;

    private final Collection<PartitionWithMetrics> allPartitionWithMetrics = new ArrayList<>();

    @Parameter public boolean enableSpeculativeExecution;

    @Parameters(name = "enableSpeculativeExecution={0}")
    public static Collection<Boolean> parameters() {
        return Arrays.asList(false, true);
    }

    @BeforeEach
    void setUp() throws IOException {
        final Path rootPath = new Path(TempDirUtils.newFolder(temporaryFolder).getAbsolutePath());
        delayedExecutor = new ScheduledExecutorServiceAdapter(EXECUTOR_RESOURCE.getExecutor());
        receivingTasks = EventReceivingTasks.createForRunningTasks();
        persistedJobEventList = new ArrayList<>();
        recoveryStarted = new AtomicBoolean();
        jobEventStore =
                new TestingFileSystemJobEventStore(
                        rootPath, new Configuration(), persistedJobEventList, recoveryStarted);

        provider =
                new SourceCoordinatorProvider<>(
                        "AdaptiveBatchSchedulerTest",
                        OPERATOR_ID,
                        new MockSource(Boundedness.BOUNDED, NUM_SPLITS),
                        1,
                        WatermarkAlignmentParams.WATERMARK_ALIGNMENT_DISABLED,
                        null);

        this.serializedJobGraph = serializeJobGraph(createDefaultJobGraph());
        allPartitionWithMetrics.clear();
    }

    @AfterEach
    void after() {
        jobEventStore.stop(true);
    }

    // This case will use job graph with the following topology:
    // Source (p=5) -- POINTWISE --> Middle (p=5) -- ALLTOALL --> Sink (p=-1)
    //
    // This case will experience the following stages:
    // 1. All source tasks are finished and all middle tasks are running
    // 2. JM failover
    // 3. After the failover, all source tasks are expected to be recovered to
    //  finished, and their produced partitions should also be restored. And middle vertex is
    // redeployed.
    @TestTemplate
    void testRecoverFromJMFailover() throws Exception {
        AdaptiveBatchScheduler scheduler = createScheduler(deserializeJobGraph(serializedJobGraph));

        runInMainThread(scheduler::startScheduling);

        runInMainThread(
                () -> {
                    // transition all sources to finished.
                    transitionExecutionsState(scheduler, ExecutionState.FINISHED, SOURCE_ID);
                });
        runInMainThread(
                () -> {
                    // transition all middle tasks to RUNNING state
                    transitionExecutionsState(scheduler, ExecutionState.INITIALIZING, MIDDLE_ID);
                    transitionExecutionsState(scheduler, ExecutionState.RUNNING, MIDDLE_ID);
                });
        List<ExecutionAttemptID> sourceExecutions =
                getCurrentAttemptIds(scheduler.getExecutionJobVertex(SOURCE_ID));
        List<ExecutionAttemptID> middleExecutions =
                getCurrentAttemptIds(scheduler.getExecutionJobVertex(MIDDLE_ID));

        Map<IntermediateResultPartitionID, Integer> subpartitionNums = new HashMap<>();
        for (ExecutionVertex vertex :
                getExecutionVertices(SOURCE_ID, scheduler.getExecutionGraph())) {

            IntermediateResultPartition partition =
                    vertex.getProducedPartitions().values().iterator().next();
            subpartitionNums.put(partition.getPartitionId(), partition.getNumberOfSubpartitions());
        }

        waitUntilWriteExecutionVertexFinishedEventPersisted(5);
        runInMainThread(() -> jobEventStore.stop(false));

        // register all produced partitions
        registerPartitions(scheduler);

        // start a new scheduler and try to recover.
        AdaptiveBatchScheduler newScheduler =
                createScheduler(deserializeJobGraph(serializedJobGraph));
        startSchedulingAndWaitRecoverFinish(newScheduler);

        // check source vertices state were recovered.
        for (ExecutionVertex vertex :
                getExecutionVertices(SOURCE_ID, newScheduler.getExecutionGraph())) {
            // check state.
            assertThat(sourceExecutions)
                    .contains(vertex.getCurrentExecutionAttempt().getAttemptId());
            assertThat(vertex.getExecutionState()).isEqualTo(ExecutionState.FINISHED);

            // check partition tracker was rebuild.
            JobMasterPartitionTracker partitionTracker =
                    ((InternalExecutionGraphAccessor) newScheduler.getExecutionGraph())
                            .getPartitionTracker();
            List<ResultPartitionID> resultPartitionIds =
                    vertex.getProducedPartitions().keySet().stream()
                            .map(
                                    ((DefaultExecutionGraph) newScheduler.getExecutionGraph())
                                            ::createResultPartitionId)
                            .collect(Collectors.toList());
            for (ResultPartitionID partitionID : resultPartitionIds) {
                assertThat(partitionTracker.isPartitionTracked(partitionID)).isTrue();
            }

            // check partitions are recovered
            IntermediateResultPartition partition =
                    vertex.getProducedPartitions().values().iterator().next();
            assertThat(partition.getNumberOfSubpartitions())
                    .isEqualTo(subpartitionNums.get(partition.getPartitionId()));
        }

        // check middle vertices state were not recovered.
        for (ExecutionVertex vertex :
                getExecutionVertices(MIDDLE_ID, newScheduler.getExecutionGraph())) {
            assertThat(middleExecutions)
                    .doesNotContain(vertex.getCurrentExecutionAttempt().getAttemptId());
            waitUntilExecutionVertexState(vertex, ExecutionState.DEPLOYING, 15000L);
        }
    }

    // This case will use job graph with the following topology:
    // Source (p=5) -- POINTWISE --> Middle (p=5) -- ALLTOALL --> Sink (p=-1)
    //
    // This case will undergo the following stages:
    // 1. All source tasks are finished, as well as middle task 0, while other middle tasks are
    // still running.
    // The middle vertex contains an operator coordinator that does not support batch snapshot.
    // 2. The partition belonging to source task0 is released because middle task0 has finished.
    // 3. JM failover.
    // 4. After failover, middle task0 and source task0 are expected be reset. Other source tasks
    // should be restored to finished and their produced partitions should also be restored.
    @TestTemplate
    void testJobVertexUnFinishedAndOperatorCoordinatorNotSupportBatchSnapshot() throws Exception {
        JobGraph jobGraph = deserializeJobGraph(serializedJobGraph);
        JobVertex jobVertex = jobGraph.findVertexByID(MIDDLE_ID);
        jobVertex.addOperatorCoordinator(
                new SerializedValue<>(
                        new TestingOperatorCoordinator.Provider(
                                jobVertex.getOperatorIDs().get(0).getGeneratedOperatorID())));
        AdaptiveBatchScheduler scheduler =
                createScheduler(
                        jobGraph,
                        Duration.ZERO /* make sure every finished event can flush on time.*/);

        runInMainThread(scheduler::startScheduling);

        runInMainThread(
                () -> {
                    // transition all sources to finished.
                    transitionExecutionsState(scheduler, ExecutionState.FINISHED, SOURCE_ID);
                });
        runInMainThread(
                () -> {
                    // transition first middle task to finished.
                    ExecutionVertex firstMiddle =
                            getExecutionVertex(MIDDLE_ID, 0, scheduler.getExecutionGraph());
                    AdaptiveBatchSchedulerTest.transitionExecutionsState(
                            scheduler,
                            ExecutionState.FINISHED,
                            Collections.singletonList(firstMiddle.getCurrentExecutionAttempt()),
                            null);
                });
        List<ExecutionAttemptID> sourceExecutions =
                getCurrentAttemptIds(scheduler.getExecutionJobVertex(SOURCE_ID));
        List<ExecutionAttemptID> middleExecutions =
                getCurrentAttemptIds(scheduler.getExecutionJobVertex(MIDDLE_ID));

        waitUntilWriteExecutionVertexFinishedEventPersisted(6);
        runInMainThread(() -> jobEventStore.stop(false));

        // register partitions, the partition of source task 0 is lost, and it will be restarted
        // if middle task 0 need be restarted.
        int subtaskIndex = 0;
        registerPartitions(
                scheduler,
                Collections.emptySet(),
                Collections.singleton(
                        scheduler
                                .getExecutionJobVertex(SOURCE_ID)
                                .getTaskVertices()[subtaskIndex]
                                .getID()));

        // start a new scheduler and try to recover.
        AdaptiveBatchScheduler newScheduler = createScheduler(jobGraph);
        startSchedulingAndWaitRecoverFinish(newScheduler);

        for (ExecutionVertex vertex :
                getExecutionVertices(SOURCE_ID, newScheduler.getExecutionGraph())) {
            // check source task0 was reset.
            if (vertex.getParallelSubtaskIndex() == subtaskIndex) {
                waitUntilExecutionVertexState(vertex, ExecutionState.DEPLOYING, 15000L);
                continue;
            }

            // check other source tasks state were recovered.
            assertThat(sourceExecutions)
                    .contains(vertex.getCurrentExecutionAttempt().getAttemptId());
            assertThat(vertex.getExecutionState()).isEqualTo(ExecutionState.FINISHED);

            // check partition tracker was rebuild.
            JobMasterPartitionTracker partitionTracker =
                    ((InternalExecutionGraphAccessor) newScheduler.getExecutionGraph())
                            .getPartitionTracker();
            List<ResultPartitionID> resultPartitionIds =
                    vertex.getProducedPartitions().keySet().stream()
                            .map(
                                    ((DefaultExecutionGraph) newScheduler.getExecutionGraph())
                                            ::createResultPartitionId)
                            .collect(Collectors.toList());
            for (ResultPartitionID partitionID : resultPartitionIds) {
                assertThat(partitionTracker.isPartitionTracked(partitionID)).isTrue();
            }
        }

        for (ExecutionVertex vertex :
                getExecutionVertices(MIDDLE_ID, newScheduler.getExecutionGraph())) {
            assertThat(middleExecutions)
                    .doesNotContain(vertex.getCurrentExecutionAttempt().getAttemptId());

            // check middle task0 is CREATED because it's waiting source task0 finished.
            if (vertex.getParallelSubtaskIndex() == subtaskIndex) {
                assertThat(vertex.getExecutionState()).isEqualTo(ExecutionState.CREATED);
                continue;
            }

            waitUntilExecutionVertexState(vertex, ExecutionState.DEPLOYING, 15000L);
        }
    }

    // This case will use job graph with the following topology:
    // Source (p=5) -- POINTWISE --> Middle (p=5) -- ALLTOALL --> Sink (p=-1)
    //
    // This case will undergo the following stages:
    // 1. All source tasks are finished.
    // The source vertex contains an operator coordinator that does not support batch snapshot.
    // 2. JM failover.
    // 3. After the failover, all source tasks are expected to be recovered to finished, and their
    // produced partitions should also be restored.
    // 4. Transition all middle task to running
    // 5. Mark the partition consumed by middle task0 as missing.
    // 6. All source task should be restarted.
    @TestTemplate
    void testJobVertexFinishedAndOperatorCoordinatorNotSupportBatchSnapshotAndPartitionNotFound()
            throws Exception {
        JobGraph jobGraph = deserializeJobGraph(serializedJobGraph);
        JobVertex jobVertex = jobGraph.findVertexByID(SOURCE_ID);
        jobVertex.addOperatorCoordinator(
                new SerializedValue<>(
                        new TestingOperatorCoordinator.Provider(
                                jobVertex.getOperatorIDs().get(0).getGeneratedOperatorID())));
        AdaptiveBatchScheduler scheduler = createScheduler(jobGraph);

        runInMainThread(scheduler::startScheduling);

        runInMainThread(
                () -> {
                    // transition all sources to finished.
                    transitionExecutionsState(scheduler, ExecutionState.FINISHED, SOURCE_ID);
                });
        List<ExecutionAttemptID> sourceExecutions =
                getCurrentAttemptIds(scheduler.getExecutionJobVertex(SOURCE_ID));

        waitUntilWriteExecutionVertexFinishedEventPersisted(5);
        runInMainThread(
                () -> {
                    jobEventStore.stop(false);
                });

        // register all produced partitions
        registerPartitions(scheduler);

        // start a new scheduler and try to recover.
        AdaptiveBatchScheduler newScheduler = createScheduler(jobGraph);
        startSchedulingAndWaitRecoverFinish(newScheduler);

        // check source vertices state were recovered.
        for (ExecutionVertex vertex :
                getExecutionVertices(SOURCE_ID, newScheduler.getExecutionGraph())) {
            // check state.
            assertThat(sourceExecutions)
                    .contains(vertex.getCurrentExecutionAttempt().getAttemptId());
            assertThat(vertex.getExecutionState()).isEqualTo(ExecutionState.FINISHED);

            // check partition tracker was rebuild.
            JobMasterPartitionTracker partitionTracker =
                    ((InternalExecutionGraphAccessor) newScheduler.getExecutionGraph())
                            .getPartitionTracker();
            List<ResultPartitionID> resultPartitionIds =
                    vertex.getProducedPartitions().keySet().stream()
                            .map(
                                    ((DefaultExecutionGraph) newScheduler.getExecutionGraph())
                                            ::createResultPartitionId)
                            .collect(Collectors.toList());
            for (ResultPartitionID partitionID : resultPartitionIds) {
                assertThat(partitionTracker.isPartitionTracked(partitionID)).isTrue();
            }
        }

        for (ExecutionVertex taskVertex :
                getExecutionVertices(MIDDLE_ID, newScheduler.getExecutionGraph())) {
            waitUntilExecutionVertexState(taskVertex, ExecutionState.DEPLOYING, 15000L);
        }

        runInMainThread(
                () -> {
                    // transition all middle tasks to running
                    transitionExecutionsState(scheduler, ExecutionState.RUNNING, MIDDLE_ID);
                });

        // trigger partition not found
        ExecutionVertex firstMiddleTask =
                getExecutionVertex(MIDDLE_ID, 0, newScheduler.getExecutionGraph());
        triggerFailedByDataConsumptionException(newScheduler, firstMiddleTask);

        waitUntilExecutionVertexState(
                getExecutionVertex(SOURCE_ID, 0, newScheduler.getExecutionGraph()),
                ExecutionState.DEPLOYING,
                15000L);

        // verify all source tasks were restarted
        for (int i = 0; i < 5; i++) {
            assertThat(
                            getExecutionVertex(SOURCE_ID, i, newScheduler.getExecutionGraph())
                                    .getExecutionState())
                    .isNotEqualTo(ExecutionState.FINISHED);
        }
    }

    // This case will use job graph with the following topology:
    // Source (p=5) -- POINTWISE --> Middle (p=5) -- ALLTOALL --> Sink (p=-1)
    //
    // This case will undergo the following stages:
    // 1. All source tasks are finished. source task0 lose its partitions.
    // 2. JM failover.
    // 3. After the failover, source task0 is expected to be reset. Other source tasks are
    // recovered to finished, and their produced partitions should also be restored.
    @TestTemplate
    void testRecoverFromJMFailoverAndPartitionsUnavailable() throws Exception {
        AdaptiveBatchScheduler scheduler = createScheduler(deserializeJobGraph(serializedJobGraph));

        runInMainThread(scheduler::startScheduling);

        runInMainThread(
                () -> {
                    // transition all sources to finished.
                    transitionExecutionsState(scheduler, ExecutionState.FINISHED, SOURCE_ID);
                });
        List<ExecutionAttemptID> sourceExecutions =
                getCurrentAttemptIds(scheduler.getExecutionJobVertex(SOURCE_ID));

        waitUntilWriteExecutionVertexFinishedEventPersisted(5);
        runInMainThread(() -> jobEventStore.stop(false));

        int losePartitionsTaskIndex = 0;

        // register partitions, the partition of source task 0 is lost, and it will be restarted
        // if middle task 0 need be restarted.
        registerPartitions(
                scheduler,
                Collections.emptySet(),
                Collections.singleton(
                        getExecutionVertex(
                                        SOURCE_ID,
                                        losePartitionsTaskIndex,
                                        scheduler.getExecutionGraph())
                                .getID()));

        // start a new scheduler and try to recover.
        AdaptiveBatchScheduler newScheduler =
                createScheduler(deserializeJobGraph(serializedJobGraph));
        startSchedulingAndWaitRecoverFinish(newScheduler);

        // check source task0 is reset and other source task are finished
        List<ExecutionVertex> sourceTasks =
                getExecutionVertices(SOURCE_ID, newScheduler.getExecutionGraph());
        for (int i = 0; i < sourceTasks.size(); i++) {
            ExecutionVertex vertex = sourceTasks.get(i);
            if (i == losePartitionsTaskIndex) {
                assertThat(sourceExecutions)
                        .doesNotContain(vertex.getCurrentExecutionAttempt().getAttemptId());
                waitUntilExecutionVertexState(vertex, ExecutionState.DEPLOYING, 15000L);
            } else {
                assertThat(sourceExecutions)
                        .contains(vertex.getCurrentExecutionAttempt().getAttemptId());
                assertThat(vertex.getExecutionState()).isEqualTo(ExecutionState.FINISHED);
            }
        }
    }

    // This case will use job graph with the following topology:
    // Source (p=5) -- POINTWISE --> Middle (p=5) -- ALLTOALL --> Sink (p=2, decided at runtime)
    @TestTemplate
    void testRecoverDecidedParallelismFromTheSameJobGraphInstance() throws Exception {
        JobGraph jobGraph = deserializeJobGraph(serializedJobGraph);

        AdaptiveBatchScheduler scheduler = createScheduler(jobGraph);

        runInMainThread(scheduler::startScheduling);

        runInMainThread(
                () -> {
                    // transition all sources to finished.
                    transitionExecutionsState(scheduler, ExecutionState.FINISHED, SOURCE_ID);
                });
        runInMainThread(
                () -> { // transition all middle tasks to finished.
                    transitionExecutionsState(scheduler, ExecutionState.FINISHED, MIDDLE_ID);
                });
        runInMainThread(
                () -> {
                    // transition all sinks to finished.
                    transitionExecutionsState(scheduler, ExecutionState.FINISHED, SINK_ID);
                });

        List<ExecutionAttemptID> sourceExecutions =
                getCurrentAttemptIds(scheduler.getExecutionJobVertex(SOURCE_ID));
        List<ExecutionAttemptID> middleExecutions =
                getCurrentAttemptIds(scheduler.getExecutionJobVertex(MIDDLE_ID));
        List<ExecutionAttemptID> sinkExecutions =
                getCurrentAttemptIds(scheduler.getExecutionJobVertex(SINK_ID));

        waitUntilWriteExecutionVertexFinishedEventPersisted(12);
        runInMainThread(() -> jobEventStore.stop(false));

        // start a new scheduler and try to recover.
        AdaptiveBatchScheduler newScheduler = createScheduler(jobGraph);
        startSchedulingAndWaitRecoverFinish(newScheduler);

        // check source vertices' state were recovered.
        for (ExecutionVertex vertex :
                getExecutionVertices(SOURCE_ID, newScheduler.getExecutionGraph())) {
            assertThat(sourceExecutions)
                    .contains(vertex.getCurrentExecutionAttempt().getAttemptId());
            assertThat(vertex.getExecutionState()).isEqualTo(ExecutionState.FINISHED);
        }
        // check middle vertices' state were recovered.
        for (ExecutionVertex vertex :
                getExecutionVertices(MIDDLE_ID, newScheduler.getExecutionGraph())) {
            assertThat(middleExecutions)
                    .contains(vertex.getCurrentExecutionAttempt().getAttemptId());
            assertThat(vertex.getExecutionState()).isEqualTo(ExecutionState.FINISHED);
        }

        // check sink's parallelism was recovered.
        assertThat(newScheduler.getExecutionJobVertex(SINK_ID).getParallelism())
                .isEqualTo(DECIDED_SINK_PARALLELISM);
        // check sink vertices' state were recovered.
        for (ExecutionVertex vertex :
                getExecutionVertices(SINK_ID, newScheduler.getExecutionGraph())) {
            assertThat(sinkExecutions).contains(vertex.getCurrentExecutionAttempt().getAttemptId());
            assertThat(vertex.getExecutionState()).isEqualTo(ExecutionState.FINISHED);
        }
    }

    // This case will use job graph with the following topology:
    // Source (p=5) -- POINTWISE --> Middle (p=5) -- ALLTOALL --> Sink (p=-1)
    //
    // This test case verifies that sourceCoordinator's split assignments are restored after a JM
    // failover, unless sources are restarted (triggered by 'partition not found' exceptions),
    // to prevent any loss of assigned splits.
    @TestTemplate
    void testPartitionNotFoundTwiceAfterJMFailover() throws Exception {
        AdaptiveBatchScheduler scheduler = createScheduler(deserializeJobGraph(serializedJobGraph));

        runInMainThread(scheduler::startScheduling);

        // assign all splits
        runInMainThread(
                () -> {
                    final SourceCoordinator<?, ?> sourceCoordinator =
                            getInternalSourceCoordinator(scheduler.getExecutionGraph(), SOURCE_ID);
                    assignSplitsForAllSubTask(
                            sourceCoordinator,
                            getCurrentAttemptIds(scheduler.getExecutionJobVertex(SOURCE_ID)));
                    // no unassigned split now.
                    checkUnassignedSplits(sourceCoordinator, 0);
                });

        // transition all sources to finished.
        runInMainThread(
                () -> transitionExecutionsState(scheduler, ExecutionState.FINISHED, SOURCE_ID));

        waitUntilWriteExecutionVertexFinishedEventPersisted(5);
        runInMainThread(
                () -> {
                    jobEventStore.stop(false);
                });

        // register all produced partitions
        registerPartitions(scheduler);

        // start a new scheduler and try to recover.
        AdaptiveBatchScheduler newScheduler =
                createScheduler(deserializeJobGraph(serializedJobGraph));
        startSchedulingAndWaitRecoverFinish(newScheduler);

        final SourceCoordinator<?, ?> sourceCoordinator =
                getInternalSourceCoordinator(newScheduler.getExecutionGraph(), SOURCE_ID);
        // no unassigned split now.
        runInMainThread(() -> checkUnassignedSplits(sourceCoordinator, 0));

        // =============================
        // FIRST TIME
        // =============================
        // trigger subtask 0 of first middle failed by dataConsumptionException.
        ExecutionVertex firstMiddle0 =
                getExecutionVertex(MIDDLE_ID, 0, newScheduler.getExecutionGraph());
        triggerFailedByDataConsumptionException(newScheduler, firstMiddle0);
        // wait until reset done.
        waitUntilExecutionVertexState(firstMiddle0, ExecutionState.CREATED, 15000L);
        // Check whether the splits have been returned.
        runInMainThread(() -> checkUnassignedSplits(sourceCoordinator, 2));

        // =============================
        // SECOND TIME
        // =============================
        // assign splits to the restarted source vertex.
        runInMainThread(
                () -> {
                    assignSplits(
                            sourceCoordinator,
                            getExecutionVertex(SOURCE_ID, 0, newScheduler.getExecutionGraph())
                                    .getCurrentExecutionAttempt()
                                    .getAttemptId());
                    // no unassigned split now.
                    checkUnassignedSplits(sourceCoordinator, 0);
                });

        // transition all sources to finished.
        runInMainThread(
                () -> transitionExecutionsState(newScheduler, ExecutionState.FINISHED, SOURCE_ID));

        // trigger subtask 1 of first middle failed by dataConsumptionException.
        ExecutionVertex firstMiddle1 =
                getExecutionVertex(MIDDLE_ID, 1, newScheduler.getExecutionGraph());
        triggerFailedByDataConsumptionException(newScheduler, firstMiddle1);
        // wait until reset done.
        waitUntilExecutionVertexState(firstMiddle1, ExecutionState.CREATED, 15000L);

        // Check whether the splits have been returned.
        runInMainThread(() -> checkUnassignedSplits(sourceCoordinator, 2));
    }

    @TestTemplate
    void testReplayEventFailed() throws Exception {
        final JobEventStore failingJobEventStore =
                new JobEventStore() {
                    @Override
                    public void start() {}

                    @Override
                    public void stop(boolean clear) {}

                    @Override
                    public void writeEvent(JobEvent event, boolean cutBlock) {}

                    @Override
                    public JobEvent readEvent() throws Exception {
                        throw new Exception();
                    }

                    @Override
                    public boolean isEmpty() {
                        return false;
                    }
                };

        final ManuallyTriggeredScheduledExecutor taskRestartExecutor =
                new ManuallyTriggeredScheduledExecutor();
        delayedExecutor = taskRestartExecutor;

        final AdaptiveBatchScheduler newScheduler =
                createScheduler(
                        deserializeJobGraph(serializedJobGraph),
                        failingJobEventStore,
                        BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_MAX_PARALLELISM
                                .defaultValue(),
                        BatchExecutionOptions.JOB_RECOVERY_SNAPSHOT_MIN_PAUSE.defaultValue());
        runInMainThread(newScheduler::startScheduling);

        // trigger scheduled restarting and drain the main thread actions
        taskRestartExecutor.triggerScheduledTasks();
        runInMainThread(() -> {});

        assertThat(
                        ExceptionUtils.findThrowableWithMessage(
                                newScheduler.getExecutionGraph().getFailureCause(),
                                "Recover failed from JM failover"))
                .isPresent();

        // source should be scheduled.
        for (ExecutionVertex vertex :
                getExecutionVertices(SOURCE_ID, newScheduler.getExecutionGraph())) {
            assertThat(vertex.getCurrentExecutionAttempt().getAttemptNumber()).isEqualTo(1);
            waitUntilExecutionVertexState(vertex, ExecutionState.DEPLOYING, 15000L);
        }
    }

    private void waitUntilWriteExecutionVertexFinishedEventPersisted(int count) throws Exception {
        CommonTestUtils.waitUntilCondition(
                () ->
                        new ArrayList<>(persistedJobEventList)
                                        .stream()
                                                .filter(
                                                        jobEvent ->
                                                                jobEvent
                                                                        instanceof
                                                                        ExecutionVertexFinishedEvent)
                                                .count()
                                == count);
    }

    private void triggerFailedByDataConsumptionException(
            SchedulerBase scheduler, ExecutionVertex executionVertex) {
        // trigger execution vertex failed by dataConsumptionException.
        runInMainThread(
                () -> {
                    // it's consumed IntermediateResultPartition.
                    IntermediateResultPartitionID partitionId =
                            getConsumedResultPartitions(
                                            scheduler.getExecutionGraph().getSchedulingTopology(),
                                            executionVertex.getID())
                                    .get(0);
                    // trigger failed.
                    AdaptiveBatchSchedulerTest.transitionExecutionsState(
                            scheduler,
                            ExecutionState.FAILED,
                            Collections.singletonList(executionVertex.getCurrentExecutionAttempt()),
                            new PartitionNotFoundException(
                                    ((DefaultExecutionGraph) scheduler.getExecutionGraph())
                                            .createResultPartitionId(partitionId)));
                });
    }

    private void assignSplits(
            SourceCoordinator<?, ?> sourceCoordinator, ExecutionAttemptID attemptId) {
        int subtask = attemptId.getSubtaskIndex();
        int attemptNumber = attemptId.getAttemptNumber();
        sourceCoordinator.executionAttemptReady(
                subtask,
                attemptNumber,
                receivingTasks.createGatewayForSubtask(subtask, attemptNumber));

        // Each source subtask assign 2 splits.
        sourceCoordinator.handleEventFromOperator(
                subtask,
                attemptNumber,
                new ReaderRegistrationEvent(subtask, "location_" + subtask));
    }

    private void assignSplitsForAllSubTask(
            SourceCoordinator<?, ?> sourceCoordinator, List<ExecutionAttemptID> attemptIds) {
        attemptIds.forEach(attemptId -> assignSplits(sourceCoordinator, attemptId));
    }

    private void checkUnassignedSplits(SourceCoordinator<?, ?> sourceCoordinator, int expected) {
        final MockSplitEnumerator newSplitEnumerator =
                (MockSplitEnumerator) sourceCoordinator.getEnumerator();

        // check splits were returned.
        runInCoordinatorThread(
                sourceCoordinator,
                () -> assertThat(newSplitEnumerator.getUnassignedSplits()).hasSize(expected));
    }

    private void runInCoordinatorThread(
            SourceCoordinator<?, ?> sourceCoordinator, Runnable runnable) {
        try {
            sourceCoordinator.getCoordinatorExecutor().submit(runnable).get();
        } catch (Exception e) {
            fail("Test failed due to " + e);
        }
    }

    private void runInMainThread(@Nonnull ThrowingRunnable<Throwable> throwingRunnable) {
        mainThreadExecutor.execute(throwingRunnable);
    }

    private void registerPartitions(AdaptiveBatchScheduler scheduler) {
        registerPartitions(scheduler, Collections.emptySet(), Collections.emptySet());
    }

    private void registerPartitions(
            AdaptiveBatchScheduler scheduler,
            Set<JobVertexID> unavailablePartitionsJobVertices,
            Set<ExecutionVertexID> unavailablePartitionsExecutionVertices) {
        // register partitions
        ExecutionGraph executionGraph = scheduler.getExecutionGraph();

        List<PartitionWithMetrics> list =
                executionGraph.getAllIntermediateResults().values().stream()
                        .flatMap(result -> Arrays.stream(result.getPartitions()))
                        .filter(
                                partition -> {
                                    ExecutionVertex producer =
                                            executionGraph
                                                    .getResultPartitionOrThrow(
                                                            partition.getPartitionId())
                                                    .getProducer();
                                    return !unavailablePartitionsJobVertices.contains(
                                                    producer.getJobvertexId())
                                            && !unavailablePartitionsExecutionVertices.contains(
                                                    producer.getID())
                                            && producer.getExecutionState()
                                                    == ExecutionState.FINISHED;
                                })
                        .map(
                                partition -> {
                                    BlockingResultInfo resultInfo =
                                            scheduler.getBlockingResultInfo(
                                                    partition.getIntermediateResult().getId());
                                    IntermediateResultPartitionID partitionId =
                                            partition.getPartitionId();
                                    final Execution producer =
                                            executionGraph
                                                    .getResultPartitionOrThrow(partitionId)
                                                    .getProducer()
                                                    .getPartitionProducer();

                                    ResultPartitionID resultPartitionID =
                                            new ResultPartitionID(
                                                    partitionId, producer.getAttemptId());

                                    DefaultShuffleMetrics metrics =
                                            new DefaultShuffleMetrics(
                                                    resultInfo == null
                                                            ? new ResultPartitionBytes(new long[0])
                                                            : new ResultPartitionBytes(
                                                                    new long
                                                                            [resultInfo
                                                                                    .getNumSubpartitions(
                                                                                            0)]));
                                    return new TestPartitionWithMetrics(resultPartitionID, metrics);
                                })
                        .collect(Collectors.toList());

        allPartitionWithMetrics.addAll(list);
    }

    private void startSchedulingAndWaitRecoverFinish(AdaptiveBatchScheduler scheduler)
            throws Exception {
        runInMainThread(scheduler::startScheduling);

        // wait recover finish
        CommonTestUtils.waitUntilCondition(
                () -> recoveryStarted.get() && !scheduler.isRecovering());
    }

    private static SourceCoordinator<?, ?> getInternalSourceCoordinator(
            final ExecutionGraph executionGraph, final JobVertexID sourceID) throws Exception {
        ExecutionJobVertex sourceJobVertex = executionGraph.getJobVertex(sourceID);
        OperatorCoordinatorHolder operatorCoordinatorHolder =
                new ArrayList<>(sourceJobVertex.getOperatorCoordinators()).get(0);
        final RecreateOnResetOperatorCoordinator coordinator =
                (RecreateOnResetOperatorCoordinator) operatorCoordinatorHolder.coordinator();
        return (SourceCoordinator<?, ?>) coordinator.getInternalCoordinator();
    }

    private static List<IntermediateResultPartitionID> getConsumedResultPartitions(
            final SchedulingTopology schedulingTopology,
            final ExecutionVertexID executionVertexId) {
        return StreamSupport.stream(
                        schedulingTopology
                                .getVertex(executionVertexId)
                                .getConsumedResults()
                                .spliterator(),
                        false)
                .map(SchedulingResultPartition::getId)
                .collect(Collectors.toList());
    }

    /** Transit the state of all executions in the Job Vertex. */
    public static void transitionExecutionsState(
            final SchedulerBase scheduler,
            final ExecutionState state,
            final JobVertexID jobVertexID) {
        AdaptiveBatchSchedulerTest.transitionExecutionsState(
                scheduler, state, scheduler.getExecutionJobVertex(jobVertexID).getJobVertex());
    }

    /**
     * Create job vertices and connect them as the following JobGraph:
     *
     * <pre>
     *  	source -|-> middle -|-> sink
     * </pre>
     *
     * <p>Parallelism of source and middle is 5.
     *
     * <p>Edge (source --> middle) is BLOCKING and POINTWISE. Edge (middle --> sink) is BLOCKING and
     * ALL_TO_ALL.
     *
     * <p>Source has an operator coordinator.
     */
    private JobGraph createDefaultJobGraph() throws IOException {
        List<JobVertex> jobVertices = new ArrayList<>();

        final JobVertex source = new JobVertex("source", SOURCE_ID);
        source.setInvokableClass(NoOpInvokable.class);
        source.addOperatorCoordinator(new SerializedValue<>(provider));
        source.setParallelism(SOURCE_PARALLELISM);
        jobVertices.add(source);

        final JobVertex middle = new JobVertex("middle", MIDDLE_ID);
        middle.setInvokableClass(NoOpInvokable.class);
        middle.setParallelism(MIDDLE_PARALLELISM);
        jobVertices.add(middle);

        final JobVertex sink = new JobVertex("sink", SINK_ID);
        sink.setInvokableClass(NoOpInvokable.class);
        jobVertices.add(sink);

        middle.connectNewDataSetAsInput(
                source, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);
        sink.connectNewDataSetAsInput(
                middle, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

        return new JobGraph(JOB_ID, "TestJob", jobVertices.toArray(new JobVertex[0]));
    }

    private static ExecutionVertex getExecutionVertex(
            final JobVertexID jobVertexId, int subtask, final ExecutionGraph executionGraph) {
        return getExecutionVertices(jobVertexId, executionGraph).get(subtask);
    }

    private static List<ExecutionVertex> getExecutionVertices(
            final JobVertexID jobVertexId, final ExecutionGraph executionGraph) {
        checkState(executionGraph.getJobVertex(jobVertexId).isInitialized());
        return Arrays.asList(executionGraph.getJobVertex(jobVertexId).getTaskVertices());
    }

    private static List<ExecutionAttemptID> getCurrentAttemptIds(
            final ExecutionJobVertex jobVertex) {
        checkState(jobVertex.isInitialized());
        return Arrays.stream(jobVertex.getTaskVertices())
                .map(executionVertex -> executionVertex.getCurrentExecutionAttempt().getAttemptId())
                .collect(Collectors.toList());
    }

    private AdaptiveBatchScheduler createScheduler(final JobGraph jobGraph) throws Exception {
        return createScheduler(
                jobGraph,
                jobEventStore,
                BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_MAX_PARALLELISM.defaultValue(),
                BatchExecutionOptions.JOB_RECOVERY_SNAPSHOT_MIN_PAUSE.defaultValue());
    }

    private AdaptiveBatchScheduler createScheduler(
            final JobGraph jobGraph, final Duration jobRecoverySnapshotMinPause) throws Exception {
        return createScheduler(
                jobGraph,
                jobEventStore,
                BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_MAX_PARALLELISM.defaultValue(),
                jobRecoverySnapshotMinPause);
    }

    private AdaptiveBatchScheduler createScheduler(
            final JobGraph jobGraph,
            final JobEventStore jobEventStore,
            int defaultMaxParallelism,
            Duration jobRecoverySnapshotMinPause)
            throws Exception {

        final ShuffleMaster<NettyShuffleDescriptor> shuffleMaster =
                new NettyShuffleMaster(
                        new ShuffleMasterContextImpl(new Configuration(), throwable -> {}));
        TestingJobMasterGateway jobMasterGateway =
                new TestingJobMasterGatewayBuilder()
                        .setGetPartitionWithMetricsFunction(
                                (timeout, set) ->
                                        CompletableFuture.completedFuture(allPartitionWithMetrics))
                        .build();
        shuffleMaster.registerJob(new JobShuffleContextImpl(jobGraph.getJobID(), jobMasterGateway));
        final JobMasterPartitionTracker partitionTracker =
                new JobMasterPartitionTrackerImpl(
                        jobGraph.getJobID(), shuffleMaster, ignored -> Optional.empty());

        Configuration jobMasterConfig = new Configuration();
        jobMasterConfig.set(
                BatchExecutionOptions.JOB_RECOVERY_SNAPSHOT_MIN_PAUSE, jobRecoverySnapshotMinPause);
        jobMasterConfig.set(BatchExecutionOptions.JOB_RECOVERY_ENABLED, true);
        jobMasterConfig.set(
                BatchExecutionOptions.JOB_RECOVERY_PREVIOUS_WORKER_RECOVERY_TIMEOUT,
                previousWorkerRecoveryTimeout);

        DefaultSchedulerBuilder schedulerBuilder =
                new DefaultSchedulerBuilder(
                                jobGraph,
                                mainThreadExecutor.getMainThreadExecutor(),
                                EXECUTOR_RESOURCE.getExecutor())
                        .setRestartBackoffTimeStrategy(
                                new FixedDelayRestartBackoffTimeStrategy
                                                .FixedDelayRestartBackoffTimeStrategyFactory(10, 0)
                                        .create())
                        .setShuffleMaster(shuffleMaster)
                        .setJobMasterConfiguration(jobMasterConfig)
                        .setPartitionTracker(partitionTracker)
                        .setDelayExecutor(delayedExecutor)
                        .setJobRecoveryHandler(
                                new DefaultBatchJobRecoveryHandler(
                                        new JobEventManager(jobEventStore), jobMasterConfig))
                        .setVertexParallelismAndInputInfosDecider(
                                createCustomParallelismDecider(DECIDED_SINK_PARALLELISM))
                        .setDefaultMaxParallelism(defaultMaxParallelism);

        return schedulerBuilder.buildAdaptiveBatchJobScheduler(enableSpeculativeExecution);
    }

    private byte[] serializeJobGraph(final JobGraph jobGraph) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream oss = new ObjectOutputStream(byteArrayOutputStream);
        oss.writeObject(jobGraph);
        return byteArrayOutputStream.toByteArray();
    }

    private JobGraph deserializeJobGraph(final byte[] serializedJobGraph) throws Exception {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(serializedJobGraph);
        ObjectInputStream ois = new ObjectInputStream(byteArrayInputStream);
        return (JobGraph) ois.readObject();
    }

    private static class TestingFileSystemJobEventStore extends FileSystemJobEventStore {

        private final List<JobEvent> persistedJobEventList;
        private final AtomicBoolean recoveryStarted;

        public TestingFileSystemJobEventStore(
                Path workingDir,
                Configuration configuration,
                List<JobEvent> persistedJobEventList,
                AtomicBoolean recoveryStarted)
                throws IOException {
            super(workingDir, configuration);
            this.persistedJobEventList = persistedJobEventList;
            this.recoveryStarted = recoveryStarted;
        }

        @Override
        protected void writeEventRunnable(JobEvent event, boolean cutBlock) {
            super.writeEventRunnable(event, cutBlock);
            persistedJobEventList.add(event);
        }

        @Override
        public JobEvent readEvent() throws Exception {
            recoveryStarted.compareAndSet(false, true);
            return super.readEvent();
        }
    }

    private static class TestPartitionWithMetrics implements PartitionWithMetrics {

        private final ResultPartitionID resultPartitionID;
        private final ShuffleMetrics metrics;

        public TestPartitionWithMetrics(
                ResultPartitionID resultPartitionID, ShuffleMetrics metrics) {
            this.resultPartitionID = resultPartitionID;
            this.metrics = metrics;
        }

        @Override
        public ShuffleMetrics getPartitionMetrics() {
            return metrics;
        }

        @Override
        public ShuffleDescriptor getPartition() {
            return new ShuffleDescriptor() {
                @Override
                public ResultPartitionID getResultPartitionID() {
                    return resultPartitionID;
                }

                @Override
                public Optional<ResourceID> storesLocalResourcesOn() {
                    return Optional.empty();
                }
            };
        }
    }
}
