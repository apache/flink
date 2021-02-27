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

package org.apache.flink.connector.base.source.hybrid;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.base.source.reader.mocks.MockBaseSource;
import org.apache.flink.runtime.highavailability.nonha.embedded.HaLeadershipControl;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.collect.ClientAndIterator;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

/** MiniCluster-based integration test for the {@link HybridSource}. */
public class HybridSourceITCase extends TestLogger {

    // Parallelism cannot exceed number of splits, otherwise test may fail intermittently with:
    // Caused by: org.apache.flink.util.FlinkException: An OperatorEvent from an
    // OperatorCoordinator to a task was lost. Triggering task failover to ensure consistency.
    // Event: '[NoMoreSplitEvent]', targetTask: Source: hybrid-source -> Map (1/4) - execution
    // #3
    private static final int PARALLELISM = 2;

    @Rule
    public final MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(PARALLELISM)
                            .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                            .withHaLeadershipControl()
                            .build());

    // ------------------------------------------------------------------------
    //  test cases
    // ------------------------------------------------------------------------

    /** Test the source in the happy path. */
    @Test
    public void testHybridSource() throws Exception {
        testHybridSource(FailoverType.NONE, sourceWithFixedSwitchPosition());
    }

    /** Test the source in the happy path with runtime position transfer. */
    @Test
    public void testHybridSourceWithDynamicSwitchPosition() throws Exception {
        testHybridSource(FailoverType.NONE, sourceWithDynamicSwitchPosition());
    }

    /** Test the source with TaskManager restart. */
    @Test
    public void testHybridSourceWithTaskManagerFailover() throws Exception {
        testHybridSource(FailoverType.TM, sourceWithFixedSwitchPosition());
    }

    /** Test the source with JobManager failover. */
    @Test
    public void testHybridSourceWithJobManagerFailover() throws Exception {
        testHybridSource(FailoverType.JM, sourceWithFixedSwitchPosition());
    }

    private Source sourceWithFixedSwitchPosition() {
        int numSplits = 2;
        int numRecordsPerSplit = EXPECTED_RESULT.size() / 4;
        return HybridSource.builder(
                        new MockBaseSource(numSplits, numRecordsPerSplit, Boundedness.BOUNDED))
                .addSource(
                        new MockBaseSource(numSplits, numRecordsPerSplit, 20, Boundedness.BOUNDED))
                .build();
    }

    private Source sourceWithDynamicSwitchPosition() {
        return HybridSource.builder(new MockBaseSource(2, 10, Boundedness.BOUNDED))
                .addSource(
                        (enumerator) -> {
                            // lazily create source here
                            return new MockBaseSource(2, 10, 20, Boundedness.BOUNDED);
                        },
                        Boundedness.BOUNDED)
                .build();
    }

    private void testHybridSource(FailoverType failoverType, Source source) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.setRestartStrategy(
                FailoverType.NONE == failoverType
                        ? RestartStrategies.noRestart()
                        : RestartStrategies.fixedDelayRestart(1, 0));

        final DataStream<Integer> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "hybrid-source")
                        .returns(Integer.class);

        final DataStream<Integer> streamFailingInTheMiddleOfReading =
                RecordCounterToFail.wrapWithFailureAfter(stream, EXPECTED_RESULT.size() / 2);

        final ClientAndIterator<Integer> client =
                DataStreamUtils.collectWithClient(
                        streamFailingInTheMiddleOfReading,
                        HybridSourceITCase.class.getSimpleName() + '-' + failoverType.name());
        final JobID jobId = client.client.getJobID();

        RecordCounterToFail.waitToFail();
        triggerFailover(
                failoverType,
                jobId,
                RecordCounterToFail::continueProcessing,
                miniClusterResource.getMiniCluster());

        final List<Integer> result = new ArrayList<>();
        while (result.size() < EXPECTED_RESULT.size() && client.iterator.hasNext()) {
            result.add(client.iterator.next());
        }

        verifyResult(result);
    }

    // ------------------------------------------------------------------------
    //  test utilities
    // ------------------------------------------------------------------------

    private enum FailoverType {
        NONE,
        TM,
        JM
    }

    private static void triggerFailover(
            FailoverType type, JobID jobId, Runnable afterFailAction, MiniCluster miniCluster)
            throws Exception {
        switch (type) {
            case NONE:
                afterFailAction.run();
                break;
            case TM:
                restartTaskManager(afterFailAction, miniCluster);
                break;
            case JM:
                triggerJobManagerFailover(jobId, afterFailAction, miniCluster);
                break;
        }
    }

    private static void triggerJobManagerFailover(
            JobID jobId, Runnable afterFailAction, MiniCluster miniCluster) throws Exception {
        final HaLeadershipControl haLeadershipControl = miniCluster.getHaLeadershipControl().get();
        haLeadershipControl.revokeJobMasterLeadership(jobId).get();
        afterFailAction.run();
        haLeadershipControl.grantJobMasterLeadership(jobId).get();
    }

    private static void restartTaskManager(Runnable afterFailAction, MiniCluster miniCluster)
            throws Exception {
        miniCluster.terminateTaskManager(0).get();
        afterFailAction.run();
        miniCluster.startTaskManager();
    }

    // ------------------------------------------------------------------------
    //  verification
    // ------------------------------------------------------------------------
    private static final List<Integer> EXPECTED_RESULT =
            IntStream.rangeClosed(0, 39).boxed().collect(Collectors.toList());

    private static void verifyResult(List<Integer> result) {
        Collections.sort(result);
        assertThat(result, equalTo(EXPECTED_RESULT));
    }

    // ------------------------------------------------------------------------
    //  mini cluster failover utilities
    // ------------------------------------------------------------------------

    private static class RecordCounterToFail {

        private static AtomicInteger records;
        private static CompletableFuture<Void> fail;
        private static CompletableFuture<Void> continueProcessing;

        private static <T> DataStream<T> wrapWithFailureAfter(DataStream<T> stream, int failAfter) {

            records = new AtomicInteger();
            fail = new CompletableFuture<>();
            continueProcessing = new CompletableFuture<>();
            return stream.map(
                    record -> {
                        final boolean halfOfInputIsRead = records.incrementAndGet() > failAfter;
                        final boolean notFailedYet = !fail.isDone();
                        if (notFailedYet && halfOfInputIsRead) {
                            fail.complete(null);
                            continueProcessing.get();
                        }
                        return record;
                    });
        }

        private static void waitToFail() throws ExecutionException, InterruptedException {
            fail.get();
        }

        private static void continueProcessing() {
            continueProcessing.complete(null);
        }
    }
}
