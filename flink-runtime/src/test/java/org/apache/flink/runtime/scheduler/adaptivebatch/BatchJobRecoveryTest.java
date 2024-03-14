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
import org.apache.flink.runtime.shuffle.JobShuffleContextImpl;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.shuffle.NettyShuffleMaster;
import org.apache.flink.runtime.shuffle.PartitionWithMetrics;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
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
import org.apache.flink.util.function.SupplierWithException;
import org.apache.flink.util.function.ThrowingRunnable;

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
        jobEventStore =
                new TestingFileSystemJobEventStore(
                        rootPath, new Configuration(), persistedJobEventList);

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

    @TestTemplate
    void testOpCoordUnsupportedBatchSnapshotWithJobVertexUnFinished() throws Exception {
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

        // start scheduling.
        runInMainThread(scheduler::startScheduling);

        runInMainThread(
                () -> {
                    // trigger all source finished
                    transitionExecutionsState(scheduler, ExecutionState.FINISHED, SOURCE_ID);
                });
        runInMainThread(
                () -> {
                    // trigger first middle finished.
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

        waitWriteJobFinishedEventCompleted(6);
        runInMainThread(
                () -> {
                    // flush events.
                    jobEventStore.stop(false);
                });

        // register partitions
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

        // check middle vertices state were not recovered.
        for (ExecutionVertex vertex :
                getExecutionVertices(MIDDLE_ID, newScheduler.getExecutionGraph())) {
            assertThat(middleExecutions)
                    .doesNotContain(vertex.getCurrentExecutionAttempt().getAttemptId());
            assertThat(vertex.getExecutionState()).isEqualTo(ExecutionState.DEPLOYING);
        }
    }

    @TestTemplate
    void testOpCoordUnsupportedBatchSnapshotWithJobVertexFinished() throws Exception {
        JobGraph jobGraph = deserializeJobGraph(serializedJobGraph);
        JobVertex jobVertex = jobGraph.findVertexByID(MIDDLE_ID);
        jobVertex.addOperatorCoordinator(
                new SerializedValue<>(
                        new TestingOperatorCoordinator.Provider(
                                jobVertex.getOperatorIDs().get(0).getGeneratedOperatorID())));
        AdaptiveBatchScheduler scheduler = createScheduler(jobGraph);

        // start scheduling.
        runInMainThread(scheduler::startScheduling);

        runInMainThread(
                () -> {
                    // trigger all source finished
                    transitionExecutionsState(scheduler, ExecutionState.FINISHED, SOURCE_ID);
                });
        runInMainThread(
                () -> {
                    // trigger all middle finished.
                    transitionExecutionsState(scheduler, ExecutionState.FINISHED, MIDDLE_ID);
                });
        List<ExecutionAttemptID> sourceExecutions =
                getCurrentAttemptIds(scheduler.getExecutionJobVertex(SOURCE_ID));
        List<ExecutionAttemptID> middleExecutions =
                getCurrentAttemptIds(scheduler.getExecutionJobVertex(MIDDLE_ID));

        waitWriteJobFinishedEventCompleted(10);
        runInMainThread(
                () -> {
                    // flush events.
                    jobEventStore.stop(false);
                });

        // register partitions
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
        }

        // check middle vertices state were recovered.
        for (ExecutionVertex vertex :
                getExecutionVertices(MIDDLE_ID, newScheduler.getExecutionGraph())) {
            // check state.
            assertThat(middleExecutions)
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
    }

    @TestTemplate
    void testOpCoordUnsupportedBatchSnapshotWithJobVertexFinishedAndPartitionNotFoundTwice()
            throws Exception {
        JobGraph jobGraph = deserializeJobGraph(serializedJobGraph);
        JobVertex jobVertex = jobGraph.findVertexByID(SOURCE_ID);
        jobVertex.addOperatorCoordinator(
                new SerializedValue<>(
                        new TestingOperatorCoordinator.Provider(
                                jobVertex.getOperatorIDs().get(0).getGeneratedOperatorID())));
        AdaptiveBatchScheduler scheduler = createScheduler(jobGraph);

        // start scheduling.
        runInMainThread(scheduler::startScheduling);

        runInMainThread(
                () -> {
                    // trigger all source finished
                    transitionExecutionsState(scheduler, ExecutionState.FINISHED, SOURCE_ID);
                });

        waitWriteJobFinishedEventCompleted(5);
        runInMainThread(
                () -> {
                    // flush events.
                    jobEventStore.stop(false);
                });

        // register partitions
        registerPartitions(scheduler);

        // start a new scheduler and try to recover.
        AdaptiveBatchScheduler newScheduler = createScheduler(jobGraph);
        startSchedulingAndWaitRecoverFinish(newScheduler);

        // check source vertices state were recovered.
        for (ExecutionVertex vertex :
                getExecutionVertices(SOURCE_ID, newScheduler.getExecutionGraph())) {
            // check state.
            assertThat(vertex.getExecutionState()).isEqualTo(ExecutionState.FINISHED);
        }

        // the first time trigger partition not found
        ExecutionVertex firstMiddle0 =
                getExecutionVertex(MIDDLE_ID, 0, newScheduler.getExecutionGraph());
        triggerFailedByDataConsumptionException(newScheduler, firstMiddle0);

        waitUntilExecutionVertexState(firstMiddle0, ExecutionState.CREATED, 5000L);

        // verify all source executionVertices were restarted
        for (int i = 0; i < 5; i++) {
            assertThat(
                            getExecutionVertex(SOURCE_ID, i, newScheduler.getExecutionGraph())
                                    .getExecutionState())
                    .isNotEqualTo(ExecutionState.FINISHED);
        }

        // trigger all source finished.
        runInMainThread(
                () -> transitionExecutionsState(newScheduler, ExecutionState.FINISHED, SOURCE_ID));

        // the second time trigger partition not found
        firstMiddle0 = getExecutionVertex(MIDDLE_ID, 0, newScheduler.getExecutionGraph());
        triggerFailedByDataConsumptionException(newScheduler, firstMiddle0);

        waitUntilExecutionVertexState(firstMiddle0, ExecutionState.CREATED, 5000L);

        // verify only source task 0 were restarted
        assertThat(
                        getExecutionVertex(SOURCE_ID, 0, newScheduler.getExecutionGraph())
                                .getExecutionState())
                .isNotEqualTo(ExecutionState.FINISHED);

        for (int i = 1; i < 5; i++) {
            assertThat(
                            getExecutionVertex(SOURCE_ID, i, newScheduler.getExecutionGraph())
                                    .getExecutionState())
                    .isEqualTo(ExecutionState.FINISHED);
        }
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
                createScheduler(deserializeJobGraph(serializedJobGraph), failingJobEventStore);
        startSchedulingAndWaitRecoverFinish(newScheduler);

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
            assertThat(vertex.getExecutionState()).isEqualTo(ExecutionState.DEPLOYING);
        }
    }

    @TestTemplate
    void testRecoverFromJMFailover() throws Exception {
        AdaptiveBatchScheduler scheduler = createScheduler(deserializeJobGraph(serializedJobGraph));

        // start scheduling.
        runInMainThread(scheduler::startScheduling);

        runInMainThread(
                () -> {
                    // trigger all source finished
                    transitionExecutionsState(scheduler, ExecutionState.FINISHED, SOURCE_ID);
                });
        runInMainThread(
                () -> {
                    // trigger all middle tasks to RUNNING state
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

        waitWriteJobFinishedEventCompleted(5);
        runInMainThread(
                () -> {
                    // flush events.
                    jobEventStore.stop(false);
                });

        // register partitions
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
            assertThat(vertex.getExecutionState()).isEqualTo(ExecutionState.DEPLOYING);
        }
    }

    @TestTemplate
    void testRecoverFromJMFailoverWithPartitionsUnavailable() throws Exception {
        AdaptiveBatchScheduler scheduler = createScheduler(deserializeJobGraph(serializedJobGraph));

        // start scheduling.
        runInMainThread(scheduler::startScheduling);

        runInMainThread(
                () -> {
                    // trigger all source finished
                    transitionExecutionsState(scheduler, ExecutionState.FINISHED, SOURCE_ID);
                });
        List<ExecutionAttemptID> sourceExecutions =
                getCurrentAttemptIds(scheduler.getExecutionJobVertex(SOURCE_ID));

        waitWriteJobFinishedEventCompleted(5);
        runInMainThread(
                () -> {
                    // flush events.
                    jobEventStore.stop(false);
                });

        // register partitions
        registerPartitions(scheduler, Collections.singleton(SOURCE_ID));

        // start a new scheduler and try to recover.
        AdaptiveBatchScheduler newScheduler =
                createScheduler(deserializeJobGraph(serializedJobGraph));
        startSchedulingAndWaitRecoverFinish(newScheduler);

        // check middle vertices state were not recovered.
        for (ExecutionVertex vertex :
                getExecutionVertices(SOURCE_ID, newScheduler.getExecutionGraph())) {
            assertThat(sourceExecutions)
                    .doesNotContain(vertex.getCurrentExecutionAttempt().getAttemptId());
            assertThat(vertex.getExecutionState()).isEqualTo(ExecutionState.DEPLOYING);
        }
    }

    @TestTemplate
    void testRecoverDecidedParallelismFromDifferentJobGraphInstance() throws Exception {
        testRecoverDecidedParallelism(() -> deserializeJobGraph(serializedJobGraph));
    }

    void testRecoverDecidedParallelism(SupplierWithException<JobGraph, Exception> jobGraphSupplier)
            throws Exception {
        AdaptiveBatchScheduler scheduler = createScheduler(jobGraphSupplier.get());

        // start scheduling.
        runInMainThread(scheduler::startScheduling);

        runInMainThread(
                () -> {
                    // trigger all source finished
                    transitionExecutionsState(scheduler, ExecutionState.FINISHED, SOURCE_ID);
                });
        runInMainThread(
                () -> { // trigger all middle finished.
                    transitionExecutionsState(scheduler, ExecutionState.FINISHED, MIDDLE_ID);
                });
        runInMainThread(
                () -> {
                    // trigger all sink finished.
                    transitionExecutionsState(scheduler, ExecutionState.FINISHED, SINK_ID);
                });

        List<ExecutionAttemptID> sourceExecutions =
                getCurrentAttemptIds(scheduler.getExecutionJobVertex(SOURCE_ID));
        List<ExecutionAttemptID> middleExecutions =
                getCurrentAttemptIds(scheduler.getExecutionJobVertex(MIDDLE_ID));
        List<ExecutionAttemptID> sinkExecutions =
                getCurrentAttemptIds(scheduler.getExecutionJobVertex(SINK_ID));

        waitWriteJobFinishedEventCompleted(12);
        runInMainThread(
                () -> {
                    // flush events.
                    jobEventStore.stop(false);
                });

        // start a new scheduler and try to recover.
        AdaptiveBatchScheduler newScheduler = createScheduler(jobGraphSupplier.get());
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

    @TestTemplate
    void testPartitionNotFoundTwiceAfterJMFailover() throws Exception {
        AdaptiveBatchScheduler scheduler = createScheduler(deserializeJobGraph(serializedJobGraph));

        // start scheduling.
        runInMainThread(scheduler::startScheduling);

        // assign all splits
        runInMainThread(
                () -> {
                    final SourceCoordinator<?, ?> sourceCoordinator =
                            getInternalSourceCoordinator(scheduler.getExecutionGraph(), SOURCE_ID);
                    assignSplitsForAllSubTask(
                            sourceCoordinator,
                            getCurrentAttemptIds(scheduler.getExecutionJobVertex(SOURCE_ID)));
                    // no unassigned splits now.
                    checkUnassignedSplits(sourceCoordinator, 0);
                });

        // trigger all source finished.
        runInMainThread(
                () -> transitionExecutionsState(scheduler, ExecutionState.FINISHED, SOURCE_ID));

        waitWriteJobFinishedEventCompleted(5);
        runInMainThread(
                () -> {
                    // flush events.
                    jobEventStore.stop(false);
                });

        // register partitions
        registerPartitions(scheduler);

        // start a new scheduler and try to recover.
        AdaptiveBatchScheduler newScheduler =
                createScheduler(deserializeJobGraph(serializedJobGraph));
        startSchedulingAndWaitRecoverFinish(newScheduler);

        triggerPartitionNotFoundTwice(newScheduler);
    }

    private void waitWriteJobFinishedEventCompleted(int count) throws Exception {
        CommonTestUtils.waitUntilCondition(
                () ->
                        persistedJobEventList.stream()
                                        .filter(
                                                jobEvent ->
                                                        jobEvent
                                                                instanceof
                                                                ExecutionVertexFinishedEvent)
                                        .count()
                                == count);
    }

    @TestTemplate
    void testPartitionNotFoundTwiceBeforeJMFailover() throws Exception {
        AdaptiveBatchScheduler scheduler = createScheduler(deserializeJobGraph(serializedJobGraph));

        // start scheduling.
        runInMainThread(scheduler::startScheduling);

        // assign all splits
        runInMainThread(
                () -> {
                    final SourceCoordinator<?, ?> sourceCoordinator =
                            getInternalSourceCoordinator(scheduler.getExecutionGraph(), SOURCE_ID);
                    assignSplitsForAllSubTask(
                            sourceCoordinator,
                            getCurrentAttemptIds(scheduler.getExecutionJobVertex(SOURCE_ID)));
                    // no unassigned splits now.
                    checkUnassignedSplits(sourceCoordinator, 0);
                });

        // trigger all source finished.
        runInMainThread(
                () -> transitionExecutionsState(scheduler, ExecutionState.FINISHED, SOURCE_ID));

        triggerPartitionNotFoundTwice(scheduler);
    }

    private void triggerPartitionNotFoundTwice(SchedulerBase scheduler) throws Exception {

        // =============================
        // FIRST TIME
        // =============================
        // trigger subtask 0 of first middle failed by dataConsumptionException.
        ExecutionVertex firstMiddle0 =
                getExecutionVertex(MIDDLE_ID, 0, scheduler.getExecutionGraph());
        triggerFailedByDataConsumptionException(scheduler, firstMiddle0);
        // wait for reset done.
        waitUntilExecutionVertexState(firstMiddle0, ExecutionState.CREATED, 5000L);

        final SourceCoordinator<?, ?> sourceCoordinator =
                getInternalSourceCoordinator(scheduler.getExecutionGraph(), SOURCE_ID);
        // Check whether the split has been returned.
        runInMainThread(() -> checkUnassignedSplits(sourceCoordinator, 2));

        // =============================
        // SECOND TIME
        // =============================
        // assign split for the restart source vertex.
        runInMainThread(
                () -> {
                    assignSplits(
                            sourceCoordinator,
                            getExecutionVertex(SOURCE_ID, 0, scheduler.getExecutionGraph())
                                    .getCurrentExecutionAttempt()
                                    .getAttemptId());
                    // no unassigned splits now.
                    checkUnassignedSplits(sourceCoordinator, 0);
                });

        // trigger all source finished.
        runInMainThread(
                () -> transitionExecutionsState(scheduler, ExecutionState.FINISHED, SOURCE_ID));

        // trigger subtask 1 of first middle failed by dataConsumptionException.
        ExecutionVertex firstMiddle1 =
                getExecutionVertex(MIDDLE_ID, 1, scheduler.getExecutionGraph());
        triggerFailedByDataConsumptionException(scheduler, firstMiddle1);
        // wait for reset done.
        waitUntilExecutionVertexState(firstMiddle1, ExecutionState.CREATED, 5000L);

        // Check whether the split has been returned.
        runInMainThread(() -> checkUnassignedSplits(sourceCoordinator, 2));
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
        registerPartitions(scheduler, Collections.emptySet());
    }

    private void registerPartitions(
            AdaptiveBatchScheduler scheduler, Set<JobVertexID> unavailablePartitionsVertices) {
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
                                    return !unavailablePartitionsVertices.contains(
                                                    producer.getJobvertexId())
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

                                    return new TestPartitionWithMetrics(
                                            resultPartitionID, resultInfo);
                                })
                        .collect(Collectors.toList());

        allPartitionWithMetrics.addAll(list);
    }

    private void startSchedulingAndWaitRecoverFinish(AdaptiveBatchScheduler scheduler)
            throws Exception {
        runInMainThread(scheduler::startScheduling);

        // wait recover start
        try {
            Thread.sleep(previousWorkerRecoveryTimeout.toMillis() + 1000);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // wait recover finish
        CommonTestUtils.waitUntilCondition(() -> !scheduler.isRecovering());
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
        return createScheduler(jobGraph, jobEventStore);
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
            final JobGraph jobGraph, final JobEventStore jobEventStore) throws Exception {
        return createScheduler(
                jobGraph,
                jobEventStore,
                BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_MAX_PARALLELISM.defaultValue());
    }

    private AdaptiveBatchScheduler createScheduler(
            final JobGraph jobGraph, final JobEventStore jobEventStore, int defaultMaxParallelism)
            throws Exception {
        return createScheduler(
                jobGraph,
                jobEventStore,
                defaultMaxParallelism,
                BatchExecutionOptions.JOB_RECOVERY_SNAPSHOT_MIN_PAUSE.defaultValue());
    }

    private AdaptiveBatchScheduler createScheduler(
            final JobGraph jobGraph,
            final JobEventStore jobEventStore,
            int defaultMaxParallelism,
            Duration jobRecoverySnapshotMinPause)
            throws Exception {

        final ShuffleMaster<NettyShuffleDescriptor> shuffleMaster =
                new NettyShuffleMaster(new Configuration());
        TestingJobMasterGateway jobMasterGateway =
                new TestingJobMasterGatewayBuilder()
                        .setGetAllPartitionWithMetricsSupplier(
                                () -> CompletableFuture.completedFuture(allPartitionWithMetrics))
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
                        .setJobEventStore(jobEventStore)
                        .setVertexParallelismAndInputInfosDecider(
                                createCustomParallelismDecider(DECIDED_SINK_PARALLELISM))
                        .setDefaultMaxParallelism(defaultMaxParallelism);

        if (enableSpeculativeExecution) {
            return schedulerBuilder.buildSpeculativeScheduler();
        } else {
            return schedulerBuilder.buildAdaptiveBatchJobScheduler();
        }
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

        public TestingFileSystemJobEventStore(
                Path workingDir, Configuration configuration, List<JobEvent> persistedJobEventList)
                throws IOException {
            super(workingDir, configuration);
            this.persistedJobEventList = persistedJobEventList;
        }

        @Override
        protected void writeEventRunnable(JobEvent event, boolean cutBlock) {
            super.writeEventRunnable(event, cutBlock);
            persistedJobEventList.add(event);
        }
    }

    private static class TestPartitionWithMetrics implements PartitionWithMetrics {

        private final ResultPartitionID resultPartitionID;
        private final BlockingResultInfo resultInfo;

        public TestPartitionWithMetrics(
                ResultPartitionID resultPartitionID, BlockingResultInfo resultInfo) {
            this.resultPartitionID = resultPartitionID;
            this.resultInfo = resultInfo;
        }

        @Override
        public ShuffleMetrics getPartitionMetrics() {
            return () ->
                    resultInfo == null
                            ? new ResultPartitionBytes(new long[0])
                            : new ResultPartitionBytes(new long[resultInfo.getNumSubpartitions(0)]);
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
