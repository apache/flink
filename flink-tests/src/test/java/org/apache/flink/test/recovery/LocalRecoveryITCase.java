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

package org.apache.flink.test.recovery;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.ListAccumulator;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HeartbeatManagerOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointingStatistics;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointingStatisticsHeaders;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.test.recovery.utils.TaskExecutorProcessEntryPoint;
import org.apache.flink.test.util.TestProcessBuilder;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLoggerExtension;
import org.apache.flink.util.concurrent.Executors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests local recovery by restarting Flink processes. */
@ExtendWith(TestLoggerExtension.class)
class LocalRecoveryITCase {

    private static final String ALLOCATION_FAILURES_ACCUMULATOR_NAME = "acc";

    @TempDir private File tmpDirectory;

    @Test
    public void testRecoverLocallyFromProcessCrashWithWorkingDirectory() throws Exception {
        final Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.ADDRESS, "localhost");
        configuration.set(JobManagerOptions.PORT, 0);
        configuration.set(RestOptions.BIND_PORT, "0");
        configuration.set(HeartbeatManagerOptions.HEARTBEAT_TIMEOUT, 10000L);
        configuration.set(HeartbeatManagerOptions.HEARTBEAT_INTERVAL, 1000L);
        configuration.set(HeartbeatManagerOptions.HEARTBEAT_RPC_FAILURE_THRESHOLD, 1);
        configuration.set(ClusterOptions.PROCESS_WORKING_DIR_BASE, tmpDirectory.getAbsolutePath());
        configuration.set(CheckpointingOptions.LOCAL_RECOVERY, true);
        configuration.set(TaskManagerOptions.SLOT_TIMEOUT, Duration.ofSeconds(30L));

        final int parallelism = 3;

        boolean success = false;
        Collection<TaskManagerProcess> taskManagerProcesses = Collections.emptyList();
        try (final StandaloneSessionClusterEntrypoint clusterEntrypoint =
                new StandaloneSessionClusterEntrypoint(configuration)) {
            clusterEntrypoint.startCluster();

            final Configuration configurationTemplate = new Configuration(configuration);
            configurationTemplate.set(JobManagerOptions.PORT, clusterEntrypoint.getRpcPort());
            taskManagerProcesses = startTaskManagerProcesses(parallelism, configurationTemplate);

            final JobClient jobClient = submitJob(parallelism, clusterEntrypoint);

            final long waitingTimeInSeconds = 45L;
            waitUntilCheckpointCompleted(
                    configuration, clusterEntrypoint.getRestPort(), jobClient.getJobID());

            restartTaskManagerProcesses(taskManagerProcesses, parallelism - 1);

            List<String> allocFailures =
                    jobClient
                            .getJobExecutionResult()
                            .get(waitingTimeInSeconds, TimeUnit.SECONDS)
                            .getAccumulatorResult(ALLOCATION_FAILURES_ACCUMULATOR_NAME);
            assertTrue(allocFailures.isEmpty(), allocFailures.toString());

            success = true;
        } finally {
            if (!success) {
                for (TaskManagerProcess taskManagerProcess : taskManagerProcesses) {
                    printLogOutput(taskManagerProcess);
                }
            }

            for (TaskManagerProcess taskManagerProcess : taskManagerProcesses) {
                taskManagerProcess.terminate();
            }
        }
    }

    private static void printLogOutput(TaskManagerProcess taskManagerProcess) {
        for (TestProcessBuilder.TestProcess testProcess : taskManagerProcess.getProcessHistory()) {
            AbstractTaskManagerProcessFailureRecoveryTest.printProcessLog(
                    taskManagerProcess.getName(), testProcess);
        }
    }

    private static void restartTaskManagerProcesses(
            Collection<TaskManagerProcess> taskManagerProcesses, int numberRestarts)
            throws IOException, InterruptedException {
        Preconditions.checkArgument(numberRestarts <= taskManagerProcesses.size());

        final Iterator<TaskManagerProcess> iterator = taskManagerProcesses.iterator();

        for (int i = 0; i < numberRestarts; i++) {
            iterator.next().restartProcess(createTaskManagerProcessBuilder());
        }
    }

    private static Collection<TaskManagerProcess> startTaskManagerProcesses(
            int numberTaskManagers, Configuration configurationTemplate) throws IOException {
        final Collection<TaskManagerProcess> result = new ArrayList<>();

        for (int i = 0; i < numberTaskManagers; i++) {
            final Configuration effectiveConfiguration = new Configuration(configurationTemplate);
            effectiveConfiguration.set(
                    TaskManagerOptions.TASK_MANAGER_RESOURCE_ID, "taskManager_" + i);

            final TestProcessBuilder.TestProcess process =
                    startTaskManagerProcess(effectiveConfiguration);

            result.add(new TaskManagerProcess(effectiveConfiguration, process));
        }

        return result;
    }

    private static TestProcessBuilder.TestProcess startTaskManagerProcess(
            Configuration effectiveConfiguration) throws IOException {
        final TestProcessBuilder taskManagerProcessBuilder = createTaskManagerProcessBuilder();
        taskManagerProcessBuilder.addConfigAsMainClassArgs(effectiveConfiguration);

        final TestProcessBuilder.TestProcess process = taskManagerProcessBuilder.start();
        return process;
    }

    @Nonnull
    private static TestProcessBuilder createTaskManagerProcessBuilder() throws IOException {
        return new TestProcessBuilder(TaskExecutorProcessEntryPoint.class.getName());
    }

    private static class TaskManagerProcess {
        private final Configuration configuration;
        private final List<TestProcessBuilder.TestProcess> processHistory;

        private TaskManagerProcess(
                Configuration configuration, TestProcessBuilder.TestProcess process) {
            this.configuration = configuration;
            this.processHistory = new ArrayList<>();
            processHistory.add(process);
        }

        Iterable<TestProcessBuilder.TestProcess> getProcessHistory() {
            return processHistory;
        }

        void restartProcess(TestProcessBuilder builder) throws IOException, InterruptedException {
            final TestProcessBuilder.TestProcess runningProcess = getRunningProcess();
            runningProcess.destroy();
            runningProcess.getProcess().waitFor();

            builder.addConfigAsMainClassArgs(configuration);
            final TestProcessBuilder.TestProcess restartedProcess = builder.start();

            processHistory.add(restartedProcess);
        }

        private TestProcessBuilder.TestProcess getRunningProcess() {
            return processHistory.get(processHistory.size() - 1);
        }

        public String getName() {
            return configuration.get(TaskManagerOptions.TASK_MANAGER_RESOURCE_ID);
        }

        public void terminate() {
            getRunningProcess().destroy();
        }
    }

    private void waitUntilCheckpointCompleted(
            Configuration configuration, int restPort, JobID jobId) throws Exception {
        final RestClient restClient = new RestClient(configuration, Executors.directExecutor());
        final JobMessageParameters messageParameters = new JobMessageParameters();
        messageParameters.jobPathParameter.resolve(jobId);

        CommonTestUtils.waitUntilCondition(
                () -> {
                    final CheckpointingStatistics checkpointingStatistics =
                            restClient
                                    .sendRequest(
                                            "localhost",
                                            restPort,
                                            CheckpointingStatisticsHeaders.getInstance(),
                                            messageParameters,
                                            EmptyRequestBody.getInstance())
                                    .join();
                    return checkpointingStatistics.getCounts().getNumberCompletedCheckpoints() > 0;
                });
    }

    private JobClient submitJob(
            int parallelism, StandaloneSessionClusterEntrypoint clusterEntrypoint)
            throws Exception {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createRemoteEnvironment(
                        "localhost", clusterEntrypoint.getRestPort(), new Configuration());
        env.setParallelism(parallelism);

        env.enableCheckpointing(100, CheckpointingMode.EXACTLY_ONCE);

        env.addSource(new LocalRecoverySource()).keyBy(x -> x).sinkTo(new DiscardingSink<>());
        final JobClient jobClient = env.executeAsync();
        return jobClient;
    }

    private static class LocalRecoverySource extends RichParallelSourceFunction<Integer>
            implements CheckpointedFunction {
        private volatile boolean running = true;

        private transient ListState<TaskNameAllocationID> previousAllocations;

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while (running) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(1);
                }

                Thread.sleep(5L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {}

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            StreamingRuntimeContext runtimeContext = (StreamingRuntimeContext) getRuntimeContext();
            String allocationId = runtimeContext.getAllocationIDAsString();
            // Pattern of the name: "Flat Map -> Sink: Unnamed (4/4)#0". Remove "#0" part:
            String myName = runtimeContext.getTaskNameWithSubtasks().split("#")[0];

            ListStateDescriptor<TaskNameAllocationID> previousAllocationsStateDescriptor =
                    new ListStateDescriptor<>("sourceState", TaskNameAllocationID.class);
            previousAllocations =
                    context.getOperatorStateStore()
                            .getUnionListState(previousAllocationsStateDescriptor);

            if (context.isRestored()) {
                final Iterable<TaskNameAllocationID> taskNameAllocationIds =
                        previousAllocations.get();

                Optional<TaskNameAllocationID> optionalMyTaskNameAllocationId = Optional.empty();

                for (TaskNameAllocationID taskNameAllocationId : taskNameAllocationIds) {
                    if (taskNameAllocationId.getName().equals(myName)) {
                        optionalMyTaskNameAllocationId = Optional.of(taskNameAllocationId);
                        break;
                    }
                }

                final TaskNameAllocationID myTaskNameAllocationId =
                        optionalMyTaskNameAllocationId.orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Could not find corresponding TaskNameAllocationID information."));

                runtimeContext.addAccumulator(
                        ALLOCATION_FAILURES_ACCUMULATOR_NAME, new ListAccumulator<String>());
                if (!allocationId.equals(myTaskNameAllocationId.getAllocationId())) {
                    runtimeContext
                            .getAccumulator(ALLOCATION_FAILURES_ACCUMULATOR_NAME)
                            .add(
                                    String.format(
                                            "The task was deployed to AllocationID(%s) but it should have been deployed to AllocationID(%s) for local recovery.",
                                            allocationId,
                                            myTaskNameAllocationId.getAllocationId()));
                }
                // terminate
                running = false;
            }

            previousAllocations.update(
                    Collections.singletonList(new TaskNameAllocationID(myName, allocationId)));
        }
    }

    private static class TaskNameAllocationID {
        private final String name;
        private final String allocationId;

        private TaskNameAllocationID(String name, String allocationId) {
            this.name = name;
            this.allocationId = allocationId;
        }

        public String getName() {
            return name;
        }

        public String getAllocationId() {
            return allocationId;
        }
    }
}
