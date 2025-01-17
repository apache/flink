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

package org.apache.flink.test.streaming.api.datastream;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.watermark.BoolWatermark;
import org.apache.flink.api.common.watermark.BoolWatermarkDeclaration;
import org.apache.flink.api.common.watermark.LongWatermark;
import org.apache.flink.api.common.watermark.LongWatermarkDeclaration;
import org.apache.flink.api.common.watermark.Watermark;
import org.apache.flink.api.common.watermark.WatermarkCombinationFunction;
import org.apache.flink.api.common.watermark.WatermarkCombinationPolicy;
import org.apache.flink.api.common.watermark.WatermarkDeclaration;
import org.apache.flink.api.common.watermark.WatermarkDeclarations;
import org.apache.flink.api.common.watermark.WatermarkHandlingResult;
import org.apache.flink.api.common.watermark.WatermarkHandlingStrategy;
import org.apache.flink.api.connector.dsv2.DataStreamV2SourceUtils;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceReader;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.datastream.api.ExecutionEnvironment;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream;
import org.apache.flink.datastream.impl.ExecutionEnvironmentImpl;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.TestingMiniCluster;
import org.apache.flink.runtime.minicluster.TestingMiniClusterConfiguration;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.runtime.watermark.AlignableBoolWatermarkDeclaration;
import org.apache.flink.util.function.SupplierWithException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * IT test case for {@link Watermark}. It will test the following behaviors of the generalized
 * watermark: (1) The aligned watermark can block upstream. (2) The long watermark combines max/min
 * correctly. (3) The bool watermark combines and/or correctly. (4) The watermark combiner will wait
 * for all channels and then combine if the {@link
 * WatermarkCombinationPolicy#isCombineWaitForAllChannels()} returns true. (5) The operator does not
 * send watermarks when the {@link OneInputStreamProcessFunction#onWatermark} returns {@link
 * WatermarkHandlingResult#POLL}. (6) The operator does not send watermarks when the {@link
 * WatermarkHandlingStrategy} is set to IGNORE and the {@link
 * OneInputStreamProcessFunction#onWatermark} returns {@link WatermarkHandlingResult#PEEK}. (7) The
 * source operator can declare and emit watermarks.
 *
 * <p>We design a test job for these test cases, the test job has four operators, each with a
 * parallelism of 2, and the shuffle edges are all to all. The job details are as follows:
 * SourceOperator (Operator1) -> ProcessOperator(Operator2) -> ProcessOperator(Operator3) ->
 * ProcessOperator(Operator4). Operator2 will declare watermarks and send watermark to downstream.
 * Operator3 will receive watermarks from the upstream Operator2 and process them according to
 * different test cases. Operator4 will receive or not receive watermarks from upstream Operator3.
 */
class WatermarkITCase {

    private static final String DEFAULT_WATERMARK_IDENTIFIER = "default";

    /** Parallelism of all operators. */
    private static final int DEFAULT_PARALLELISM = 2;

    private MiniCluster flinkCluster;

    /**
     * The source operator will emit integer data within a range of 0 to {@code
     * NUMBER_KEYS}(exclusive).
     */
    private static final int NUMBER_KEYS = 100;

    public void startMiniCluster() throws Exception {
        TestingMiniClusterConfiguration miniClusterConfiguration =
                TestingMiniClusterConfiguration.newBuilder()
                        .setNumTaskManagers(3)
                        .setNumSlotsPerTaskManager(4)
                        .build();
        flinkCluster = TestingMiniCluster.newBuilder(miniClusterConfiguration).build();
        flinkCluster.start();
    }

    @BeforeEach
    void before() throws Exception {
        startMiniCluster();
    }

    @AfterEach
    void after() throws Exception {
        Operator2ProcessFunction.clear();
        Operator3ProcessFunction.clear();
        Operator4ProcessFunction.clear();

        if (flinkCluster != null) {
            flinkCluster.close();
            flinkCluster = null;
        }
    }

    /**
     * Test aligned watermark can block upstream. In this test case, Operator2 will declare the
     * aligned watermark. We will block Operator2's subtasks first and unblock them step by step,
     * and each subtask will emit a watermark with value {@code true} after being unblocked. The
     * Operator3 should receive only one combined and aligned watermark with value {@code true}
     * after both the Operator2's subtask are unblocked.
     */
    @Test
    void testAlignedWatermarkBlockUpstream() throws Exception {
        StreamGraph streamGraph = getStreamGraphForAlignedWatermark(Map.of(0, true, 1, true));

        // block operator2's subtask 0 and subtask 1
        Operator2ProcessFunction.blockSubTasks(0, 1);

        // submit job
        JobID jobId = flinkCluster.submitJob(streamGraph).get().getJobID();

        // wait all operator3 tasks have started
        tryWaitUntilCondition(
                () -> Operator3ProcessFunction.attemptIds.size() == DEFAULT_PARALLELISM);
        // since all Operator2 tasks are blocked, so the Operator3 should not receive watermarks and
        // records
        assertOperatorReceivedWatermarkValues(Operator3ProcessFunction.receivedWatermarks, false);
        assertThat(Operator3ProcessFunction.receivedRecords).isEmpty();

        // unblock operator2's subtask 0; it will send a watermark with the value {@code true} to
        // operator3.
        // However, operator3 should not receive the watermark since it is not aligned, and it
        // should block the input until the watermark is aligned.
        Operator2ProcessFunction.unblockSubTasks(0);
        Thread.sleep(1000);
        assertOperatorReceivedWatermarkValues(Operator3ProcessFunction.receivedWatermarks, false);
        assertThat(Operator3ProcessFunction.receivedRecords).isEmpty();

        // unblock operator2's subtask 1, it will send a watermark with the value true to operator3.
        // Operator3 should receive the combined watermark with the value {@code true} and not block
        // the input as the watermark has already been aligned.
        Operator2ProcessFunction.unblockSubTasks(1);

        // wait job complete
        JobResult jobResult = flinkCluster.requestJobResult(jobId).get();
        assertThat(jobResult.getSerializedThrowable()).isEmpty();

        // check Operator3 receives only one watermark per task: true
        assertOperatorReceivedWatermarkValues(
                Operator3ProcessFunction.receivedWatermarks, false, true);
        checkSinkResults();
    }

    /**
     * Test long watermark combines correctly using max function. In this test case, Operator2 will
     * declare the long watermark using the combine function max. We will block Operator2's subtasks
     * first and unblock them step by step, the subtask 0 will emit a watermark with value {@code
     * 1L} after being unblocked, the subtask 1 will emit a watermark with value {@code 2L} after
     * being unblocked. The Operator3 should receive two watermarks with the value of {@code 1L} and
     * {@code 2L} after both the Operator2's subtask are unblocked.
     */
    @Test
    void testLongWatermarkCombineMax()
            throws ReflectiveOperationException, ExecutionException, InterruptedException {
        StreamGraph streamGraph =
                getStreamGraphForLongWatermarkCombineFunction(true, Map.of(0, 1L, 1, 2L));

        // block operator2's subtask 0 and subtask 1
        Operator2ProcessFunction.blockSubTasks(0, 1);

        // submit job
        JobID jobId = flinkCluster.submitJob(streamGraph).get().getJobID();

        // wait all operator3 tasks have started
        tryWaitUntilCondition(
                () -> Operator3ProcessFunction.attemptIds.size() == DEFAULT_PARALLELISM);
        // since all Operator2 tasks are blocked, so the Operator3 should not receive watermarks and
        // records
        assertOperatorReceivedWatermarkValues(Operator3ProcessFunction.receivedWatermarks, true);
        assertThat(Operator3ProcessFunction.receivedRecords).isEmpty();

        // unblock Operator2's subtask 0, which will send a watermark with value {@code 1L} to
        // Operator3,
        // Operator3 will receive the watermark with value {@code 1L} from channel 0.
        Operator2ProcessFunction.unblockSubTasks(0);
        tryWaitUntilCondition(
                () ->
                        checkOperatorReceivedWatermarksAllNotEmpty(
                                Operator3ProcessFunction.receivedWatermarks));
        assertOperatorReceivedWatermarkValues(
                Operator3ProcessFunction.receivedWatermarks, true, 1L);

        // unblock operator2's subtask 1, which will send watermark with value {@code 2L} to
        // Operator3.
        // for Operator3, since the watermark combine function is max, and the channel 1 received
        // watermark value 2L is larger than chanel 0 received watermark value 1L,
        // so the Operator3 will receive combined watermark with value {@code 2L}.
        Operator2ProcessFunction.unblockSubTasks(1);

        // wait job complete
        JobResult jobResult = flinkCluster.requestJobResult(jobId).get();
        assertThat(jobResult.getSerializedThrowable()).isEmpty();

        // check Operator3 receives two watermark per task: 1L and 2L
        assertOperatorReceivedWatermarkValues(
                Operator3ProcessFunction.receivedWatermarks, true, 1L, 2L);

        checkSinkResults();
    }

    /**
     * Test long watermark combines correctly using min function. In this test case, Operator2 will
     * declare the long watermark using the combine function min. We will block Operator2's subtasks
     * first and unblock them step by step, the subtask 0 will emit a watermark with value {@code
     * 1L} after being unblocked, the subtask 1 will emit a watermark with value {@code 2L} after
     * being unblocked. The Operator3 should receive only one watermarks with the value of {@code
     * 1L} after both the Operator2's subtask are unblocked.
     */
    @Test
    void testLongWatermarkCombineMin()
            throws ReflectiveOperationException, ExecutionException, InterruptedException {
        StreamGraph streamGraph =
                getStreamGraphForLongWatermarkCombineFunction(false, Map.of(0, 1L, 1, 2L));

        // block operator2's subtask 0 and subtask 1
        Operator2ProcessFunction.blockSubTasks(0, 1);

        // submit job
        JobID jobId = flinkCluster.submitJob(streamGraph).get().getJobID();

        // wait all operator3 tasks have started
        tryWaitUntilCondition(
                () -> Operator3ProcessFunction.attemptIds.size() == DEFAULT_PARALLELISM);
        // since all Operator2 tasks are blocked, so the Operator3 should not receive watermarks and
        // records
        assertOperatorReceivedWatermarkValues(Operator3ProcessFunction.receivedWatermarks, true);
        assertThat(Operator3ProcessFunction.receivedRecords).isEmpty();

        // unblock Operator2's subtask 0, which will send a watermark with value {@code 1L} to
        // Operator3,
        // Operator3 will receive the watermark with value {@code 1L} from channel 0
        Operator2ProcessFunction.unblockSubTasks(0);
        tryWaitUntilCondition(
                () ->
                        checkOperatorReceivedWatermarksAllNotEmpty(
                                Operator3ProcessFunction.receivedWatermarks));
        assertOperatorReceivedWatermarkValues(
                Operator3ProcessFunction.receivedWatermarks, true, 1L);

        // unblock operator2's subtask 1, which will send watermark with value {@code 2L} to
        // Operator3.
        // for Operator3, since the watermark combine function is min, and the channel 1 received
        // watermark value 2L is larger than channel 0 received watermark value 1L,
        // so the combined watermark will have value {@code 1L}.
        // since the value is same as the previous one, the Operator3 will not receive the watermark
        // again.
        Operator2ProcessFunction.unblockSubTasks(1);

        // wait job complete
        JobResult jobResult = flinkCluster.requestJobResult(jobId).get();
        assertThat(jobResult.getSerializedThrowable()).isEmpty();

        // check Operator3 receives one watermark per task: 1L
        assertOperatorReceivedWatermarkValues(
                Operator3ProcessFunction.receivedWatermarks, true, 1L);

        checkSinkResults();
    }

    /**
     * Test bool watermark combines correctly using and function. In this test case, Operator2 will
     * declare the long watermark using the combine function {@code AND}. We will block Operator2's
     * subtasks first and unblock them step by step, the subtask 0 will emit a watermark with value
     * {@code false} after being unblocked, the subtask 1 will emit a watermark with value {@code
     * true} after being unblocked. The Operator3 should receive only one watermark with the value
     * of {@code false} after both the Operator2's subtask are unblocked.
     */
    @Test
    void testBoolWatermarkCombineAnd()
            throws ReflectiveOperationException, ExecutionException, InterruptedException {
        StreamGraph streamGraph =
                getStreamGraphForBoolWatermarkCombineFunction(true, Map.of(0, false, 1, true));

        // block operator2's subtask 0 and subtask 1
        Operator2ProcessFunction.blockSubTasks(0, 1);

        // submit job
        JobID jobId = flinkCluster.submitJob(streamGraph).get().getJobID();

        // wait all operator3 tasks have started
        tryWaitUntilCondition(
                () -> Operator3ProcessFunction.attemptIds.size() == DEFAULT_PARALLELISM);
        // since all Operator2 tasks are blocked, so the Operator3 should not receive watermarks and
        // records
        assertOperatorReceivedWatermarkValues(Operator3ProcessFunction.receivedWatermarks, false);
        assertThat(Operator3ProcessFunction.receivedRecords).isEmpty();

        // unblock Operator2's subtask 0, which will send a watermark with value {@code false} to
        // Operator3,
        // Operator3 will receive the watermark with value {@code false} from channel 0.
        Operator2ProcessFunction.unblockSubTasks(0);
        tryWaitUntilCondition(
                () ->
                        checkOperatorReceivedWatermarksAllNotEmpty(
                                Operator3ProcessFunction.receivedWatermarks));
        assertOperatorReceivedWatermarkValues(
                Operator3ProcessFunction.receivedWatermarks, false, false);

        // unblock operator2's subtask 1, which will send watermark with value {@code true} to
        // Operator3.
        // for Operator3, since the watermark combine function is and, the combined result of
        // channel 1 received watermark value true and with channel 0 received watermark value false
        // is {@code false},
        // since the value is same as the previous one, the Operator3 will not receive the watermark
        // again.
        Operator2ProcessFunction.unblockSubTasks(1);

        // wait job complete
        JobResult jobResult = flinkCluster.requestJobResult(jobId).get();
        assertThat(jobResult.getSerializedThrowable()).isEmpty();

        // check Operator3 receives one watermark per task: 1L
        assertOperatorReceivedWatermarkValues(
                Operator3ProcessFunction.receivedWatermarks, false, false);

        checkSinkResults();
    }

    /**
     * Test bool watermark combines correctly using and function. In this test case, Operator2 will
     * declare the long watermark using the combine function {@code OR}. We will block Operator2's
     * subtasks first and unblock them step by step, the subtask 0 will emit a watermark with value
     * {@code false} after being unblocked, the subtask 1 will emit a watermark with value {@code
     * true} after being unblocked. The Operator3 should receive two watermarks with the value of
     * {@code false} and {@code true} after both the Operator2's subtask are unblocked.
     */
    @Test
    void testBoolWatermarkCombineOr()
            throws ReflectiveOperationException, ExecutionException, InterruptedException {
        StreamGraph streamGraph =
                getStreamGraphForBoolWatermarkCombineFunction(false, Map.of(0, false, 1, true));

        // block operator2's subtask 0 and subtask 1
        Operator2ProcessFunction.blockSubTasks(0, 1);

        // submit job
        JobID jobId = flinkCluster.submitJob(streamGraph).get().getJobID();

        // wait all operator3 tasks have started
        tryWaitUntilCondition(
                () -> Operator3ProcessFunction.attemptIds.size() == DEFAULT_PARALLELISM);
        // since all Operator2 tasks are blocked, so the Operator3 should not receive watermarks and
        // records
        assertOperatorReceivedWatermarkValues(Operator3ProcessFunction.receivedWatermarks, false);
        assertThat(Operator3ProcessFunction.receivedRecords).isEmpty();

        // unblock Operator2's subtask 0, which will send a watermark with value {@code false} to
        // Operator3,
        // Operator3 will receive the watermark with value {@code false} from channel 0.
        Operator2ProcessFunction.unblockSubTasks(0);
        tryWaitUntilCondition(
                () ->
                        checkOperatorReceivedWatermarksAllNotEmpty(
                                Operator3ProcessFunction.receivedWatermarks));
        assertOperatorReceivedWatermarkValues(
                Operator3ProcessFunction.receivedWatermarks, false, false);

        // unblock operator2's subtask 1, which will send watermark with value {@code true} to
        // Operator3.
        // for Operator3, since the watermark combine function is and, the combined result of
        // channel 1 received watermark value true and with channel 0 received watermark value false
        // is {@code true},
        // since the Operator3 will receive the watermark with value {@code true} again.
        Operator2ProcessFunction.unblockSubTasks(1);

        // wait job complete
        JobResult jobResult = flinkCluster.requestJobResult(jobId).get();
        assertThat(jobResult.getSerializedThrowable()).isEmpty();

        // check Operator3 receives one watermark per task: 1L
        assertOperatorReceivedWatermarkValues(
                Operator3ProcessFunction.receivedWatermarks, false, false, true);

        checkSinkResults();
    }

    /**
     * Test watermark combiner will wait for all channels and then combine if the {@link
     * WatermarkCombinationPolicy#isCombineWaitForAllChannels()} returns true. In this test case,
     * Operator2 will declare the long watermark and make the {@link
     * WatermarkCombinationPolicy#isCombineWaitForAllChannels()} return true, the combine function
     * will be {@code MAX}. We will block Operator2's subtasks first and unblock them step by step,
     * the subtask 0 will emit a watermark with value {@code 1L} after being unblocked, the subtask
     * 1 will emit a watermark with value {@code 2L} after being unblocked. The Operator3 should
     * receive only one watermark with the value of {@code 2L} after both the Operator2's subtask
     * are unblocked.
     */
    @Test
    void testCombineWaitForAllChannels()
            throws ReflectiveOperationException, ExecutionException, InterruptedException {
        StreamGraph streamGraph = getStreamGraphForCombineWaitForAllChannels(Map.of(0, 1L, 1, 2L));

        // block operator2's subtask 0 and subtask 1
        Operator2ProcessFunction.blockSubTasks(0, 1);

        // submit job
        JobID jobId = flinkCluster.submitJob(streamGraph).get().getJobID();

        // wait all operator3 tasks have started
        tryWaitUntilCondition(
                () -> Operator3ProcessFunction.attemptIds.size() == DEFAULT_PARALLELISM);
        // since all Operator2 tasks are blocked, so the Operator3 should not receive watermarks and
        // records
        assertOperatorReceivedWatermarkValues(Operator3ProcessFunction.receivedWatermarks, true);
        assertThat(Operator3ProcessFunction.receivedRecords).isEmpty();

        // unblock Operator2's subtask 0, which will send a watermark with value {@code 1L} to
        // Operator3,
        // for Operator3, only the channel 0 received one watermark with value {@code 1L}, while the
        // channel 1 does not receive any watermark.
        // as a result, the Operator3 will not receive the watermark.
        Operator2ProcessFunction.unblockSubTasks(0);
        Thread.sleep(1000);
        assertOperatorReceivedWatermarkValues(Operator3ProcessFunction.receivedWatermarks, true);

        // unblock operator2's subtask 1, which will send watermark with value {@code 2L} to
        // Operator3.
        // for Operator3, since the watermark combine function is max, and only the channel 0
        // received one watermark with value {@code 1L}, the channel 1 receive one watermark with
        // value {@code 2L},
        // so the Operator3 will receive one combined watermark with value {@code 2L}.
        Operator2ProcessFunction.unblockSubTasks(1);

        // wait job complete
        JobResult jobResult = flinkCluster.requestJobResult(jobId).get();
        assertThat(jobResult.getSerializedThrowable()).isEmpty();

        // check Operator3 receives two watermark per task: 1L and 2L
        assertOperatorReceivedWatermarkValues(
                Operator3ProcessFunction.receivedWatermarks, true, 2L);

        checkSinkResults();
    }

    /**
     * Test operator does not send watermarks when the {@link
     * OneInputStreamProcessFunction#onWatermark} returns {@link WatermarkHandlingResult#POLL}. In
     * this test case, we will not block any operators. The Operator2 will declare the long
     * watermark. The Operator3 will receive watermark and process them in {@link
     * Operator3ProcessFunction#onWatermark}, this method will return {@link
     * WatermarkHandlingResult#POLL}. The Operator4 should not receive any watermarks, since the
     * watermarks has been process and poll by user in Operator3.
     */
    @Test
    void testWatermarkHandlingResultIsPoll() throws Exception {
        StreamGraph streamGraph =
                getStreamGraphForWatermarkHandlingResultIsPoll(Map.of(0, 1L, 1, 2L));

        // submit job
        JobID jobId = flinkCluster.submitJob(streamGraph).get().getJobID();
        // wait job complete
        JobResult jobResult = flinkCluster.requestJobResult(jobId).get();
        assertThat(jobResult.getSerializedThrowable()).isEmpty();

        // check operator 4 receive no matermark
        assertOperatorReceivedWatermarkValues(Operator4ProcessFunction.receivedWatermarks, true);

        checkSinkResults();
    }

    /**
     * Test the operator does not send watermarks when the {@link WatermarkHandlingStrategy} is set
     * to IGNORE and the {@link OneInputStreamProcessFunction#onWatermark} returns {@link
     * WatermarkHandlingResult#PEEK}. In this test case, we will not block any operators. The
     * Operator2 will declare the long watermark with {@link WatermarkHandlingStrategy#IGNORE}. The
     * Operator3 will receive watermark and process them in {@link
     * Operator3ProcessFunction#onWatermark}, this method will return {@link
     * WatermarkHandlingResult#PEEK}. The Operator4 should not receive any watermarks, since the
     * watermarks has been ignored in Operator3.
     */
    @Test
    void testDefaultHandlingStrategyIgnore() throws Exception {
        StreamGraph streamGraph =
                getStreamGraphForDefaultHandlingStrategyIgnore(Map.of(0, 1L, 1, 2L));

        // submit job
        JobID jobId = flinkCluster.submitJob(streamGraph).get().getJobID();
        // wait job complete
        JobResult jobResult = flinkCluster.requestJobResult(jobId).get();
        assertThat(jobResult.getSerializedThrowable()).isEmpty();

        // check Operator4 should not receive any watermarks
        assertOperatorReceivedWatermarkValues(Operator4ProcessFunction.receivedWatermarks, true);

        checkSinkResults();
    }

    /**
     * Test the source operator can declare and emit watermarks. In this test case, the test job
     * will contain two operator: SourceOperator1 and Operator2. The SourceOperator1 will declare
     * and emit long watermarks. Operator2 should receive the emitted watermarks.
     */
    @Test
    void testSourceDeclareAndEmitWatermark() throws Exception {
        StreamGraph streamGraph =
                getStreamGraphForSourceDeclareAndEmitWatermarks(Map.of(0, 1L, 1, 1L));

        // submit job
        JobID jobId = flinkCluster.submitJob(streamGraph).get().getJobID();
        // wait job complete
        JobResult jobResult = flinkCluster.requestJobResult(jobId).get();
        assertThat(jobResult.getSerializedThrowable()).isEmpty();

        // check Operator2 should receive watermarks
        assertOperatorReceivedWatermarkValues(
                Operator2ProcessFunction.receivedWatermarks, true, 1L);
    }

    public static class Operator1SourceReader extends IteratorSourceReader {

        public SourceReaderContext sourceReaderContext;
        public Map<Integer, Watermark> emitWatermark;
        private boolean isFirstSendWatermark = true;

        public Operator1SourceReader(
                SourceReaderContext context, Map<Integer, Watermark> emitWatermark) {
            super(context);
            this.sourceReaderContext = context;
            this.emitWatermark = emitWatermark;
        }

        @Override
        public InputStatus pollNext(ReaderOutput output) {
            InputStatus inputStatus = super.pollNext(output);

            int indexOfSubtask = sourceReaderContext.getIndexOfSubtask();
            if (isFirstSendWatermark
                    && emitWatermark != null
                    && emitWatermark.containsKey(indexOfSubtask)) {
                sourceReaderContext.emitWatermark(emitWatermark.get(indexOfSubtask));
                isFirstSendWatermark = false;
            }

            return inputStatus;
        }
    }

    public static class Operator1Source extends NumberSequenceSource {

        public WatermarkDeclaration watermarkDeclaration;
        public Map<Integer, Watermark> subTaskId2needEmittedWatermark;

        public Operator1Source(
                long from,
                long to,
                WatermarkDeclaration watermarkDeclaration,
                Map<Integer, Watermark> subTaskId2needEmittedWatermark) {
            super(from, to);
            this.watermarkDeclaration = watermarkDeclaration;
            this.subTaskId2needEmittedWatermark = subTaskId2needEmittedWatermark;
        }

        @Override
        public Set<? extends WatermarkDeclaration> declareWatermarks() {
            return Set.of(watermarkDeclaration);
        }

        @Override
        public SourceReader<Long, NumberSequenceSplit> createReader(
                SourceReaderContext readerContext) {
            return new Operator1SourceReader(readerContext, subTaskId2needEmittedWatermark);
        }
    }

    public static class Operator2ProcessFunction
            implements OneInputStreamProcessFunction<Long, Long> {
        public static Map<Integer, Boolean> subtaskBlocked = new ConcurrentHashMap<>();
        public static Map<Integer, Integer> attemptIds = new ConcurrentHashMap<>();
        public static ConcurrentLinkedQueue<Long> receivedRecords = new ConcurrentLinkedQueue<>();
        // subtask id -> received watermarks
        public static Map<Integer, List<Watermark>> receivedWatermarks = new ConcurrentHashMap<>();
        protected WatermarkDeclaration watermarkDeclaration;
        // subtask id -> emit watermark
        protected Map<Integer, Watermark> subTaskId2needEmittedWatermark;

        public Operator2ProcessFunction(
                @Nullable WatermarkDeclaration watermarkDeclaration,
                @Nullable Map<Integer, Watermark> subTaskId2needEmittedWatermark) {
            this.watermarkDeclaration = watermarkDeclaration;
            this.subTaskId2needEmittedWatermark = subTaskId2needEmittedWatermark;
        }

        @Override
        public void open(NonPartitionedContext<Long> ctx) throws Exception {
            int subIdx = ctx.getTaskInfo().getIndexOfThisSubtask();

            // attempt id ++
            attemptIds.compute(
                    subIdx,
                    (ignored, value) -> {
                        if (value == null) {
                            value = 0;
                        } else {
                            value += 1;
                        }
                        return value;
                    });

            receivedWatermarks.computeIfAbsent(subIdx, taskIndex -> new ArrayList<>());

            // wait until unblocked.
            if (subtaskBlocked.containsKey(subIdx) && subtaskBlocked.get(subIdx)) {
                tryWaitUntilCondition(() -> !subtaskBlocked.get(subIdx));
            }

            if (subTaskId2needEmittedWatermark != null
                    && subTaskId2needEmittedWatermark.containsKey(subIdx)) {
                ctx.getWatermarkManager().emitWatermark(subTaskId2needEmittedWatermark.get(subIdx));
            }
        }

        @Override
        public void processRecord(Long record, Collector<Long> output, PartitionedContext<Long> ctx)
                throws Exception {
            receivedRecords.add(record);
            output.collect(record * 2);
        }

        @Override
        public WatermarkHandlingResult onWatermark(
                Watermark watermark, Collector<Long> output, NonPartitionedContext<Long> ctx) {
            int subIdx = ctx.getTaskInfo().getIndexOfThisSubtask();
            receivedWatermarks.get(subIdx).add(watermark);
            return WatermarkHandlingResult.PEEK;
        }

        @Override
        public Set<? extends WatermarkDeclaration> declareWatermarks() {
            if (watermarkDeclaration != null) {
                return Set.of(watermarkDeclaration);
            }
            return Collections.emptySet();
        }

        public static void blockSubTasks(Integer... subIndices) {
            setSubtaskBlocked(Arrays.asList(subIndices), true, subtaskBlocked);
        }

        public static void unblockSubTasks(Integer... subIndices) {
            setSubtaskBlocked(Arrays.asList(subIndices), false, subtaskBlocked);
        }

        public static void clear() {
            subtaskBlocked.clear();
            attemptIds.clear();
            receivedRecords.clear();
            receivedWatermarks.clear();
        }
    }

    public static class Operator3ProcessFunction
            implements OneInputStreamProcessFunction<Long, Long> {
        public static Map<Integer, Integer> attemptIds = new ConcurrentHashMap<>();
        public static ConcurrentLinkedQueue<Long> receivedRecords = new ConcurrentLinkedQueue<>();
        // subtask id -> received watermarks
        public static Map<Integer, List<Watermark>> receivedWatermarks = new ConcurrentHashMap<>();
        public WatermarkHandlingResult watermarkHandlingResult;

        public Operator3ProcessFunction(WatermarkHandlingResult watermarkHandlingResult) {
            this.watermarkHandlingResult = watermarkHandlingResult;
        }

        @Override
        public void open(NonPartitionedContext<Long> ctx) throws Exception {
            int subIdx = ctx.getTaskInfo().getIndexOfThisSubtask();

            // attempt id ++
            attemptIds.compute(
                    subIdx,
                    (ignored, value) -> {
                        if (value == null) {
                            value = 0;
                        } else {
                            value += 1;
                        }
                        return value;
                    });
            receivedWatermarks.computeIfAbsent(subIdx, taskIndex -> new ArrayList<>());
        }

        @Override
        public void processRecord(Long record, Collector<Long> output, PartitionedContext<Long> ctx)
                throws Exception {
            receivedRecords.add(record);
            output.collect(record + 1);
        }

        @Override
        public WatermarkHandlingResult onWatermark(
                Watermark watermark, Collector<Long> output, NonPartitionedContext<Long> ctx) {
            int subIdx = ctx.getTaskInfo().getIndexOfThisSubtask();
            receivedWatermarks.get(subIdx).add(watermark);
            return watermarkHandlingResult;
        }

        public static void clear() {
            attemptIds.clear();
            receivedRecords.clear();
            receivedWatermarks.clear();
        }
    }

    public static class Operator4ProcessFunction
            implements OneInputStreamProcessFunction<Long, Long> {
        public static Map<Integer, Integer> attemptIds = new ConcurrentHashMap<>();
        public static ConcurrentLinkedQueue<Long> receivedRecords = new ConcurrentLinkedQueue<>();
        // subtask id -> received watermarks
        public static Map<Integer, List<Watermark>> receivedWatermarks = new ConcurrentHashMap<>();

        @Override
        public void open(NonPartitionedContext<Long> ctx) throws Exception {
            int subIdx = ctx.getTaskInfo().getIndexOfThisSubtask();

            // attempt id ++
            attemptIds.compute(
                    subIdx,
                    (ignored, value) -> {
                        if (value == null) {
                            value = 0;
                        } else {
                            value += 1;
                        }
                        return value;
                    });
            receivedWatermarks.computeIfAbsent(subIdx, taskIndex -> new ArrayList<>());
        }

        @Override
        public void processRecord(Long record, Collector<Long> output, PartitionedContext<Long> ctx)
                throws Exception {
            receivedRecords.add(record);
        }

        @Override
        public WatermarkHandlingResult onWatermark(
                Watermark watermark, Collector<Long> output, NonPartitionedContext<Long> ctx) {
            int subIdx = ctx.getTaskInfo().getIndexOfThisSubtask();
            receivedWatermarks.get(subIdx).add(watermark);
            return WatermarkHandlingResult.PEEK;
        }

        public static void clear() {
            attemptIds.clear();
            receivedRecords.clear();
            receivedWatermarks.clear();
        }
    }

    public StreamGraph getStreamGraphForTestcase(
            WatermarkDeclaration watermarkDeclaration,
            Map<Integer, Watermark> subTaskId2needEmittedWatermarkInOperator2,
            WatermarkHandlingResult operator3HandlingResult)
            throws ReflectiveOperationException {
        ExecutionEnvironmentImpl env =
                (ExecutionEnvironmentImpl) ExecutionEnvironment.getInstance();
        env.getConfiguration().set(PipelineOptions.OPERATOR_CHAINING, false);
        ProcessConfigurableAndNonKeyedPartitionStream<Long> source =
                env.fromSource(
                                DataStreamV2SourceUtils.fromData(
                                        LongStream.range(0, NUMBER_KEYS)
                                                .boxed()
                                                .collect(Collectors.toList())),
                                "Operator1")
                        .withParallelism(DEFAULT_PARALLELISM);

        source.shuffle()
                .process(
                        new Operator2ProcessFunction(
                                watermarkDeclaration, subTaskId2needEmittedWatermarkInOperator2))
                .withName("Operator2")
                .withParallelism(DEFAULT_PARALLELISM)
                .shuffle()
                .process(new Operator3ProcessFunction(operator3HandlingResult))
                .withName("Operator3")
                .withParallelism(DEFAULT_PARALLELISM)
                .shuffle()
                .process(new Operator4ProcessFunction())
                .withName("Operator4")
                .withParallelism(DEFAULT_PARALLELISM);
        return env.getStreamGraph();
    }

    public StreamGraph getStreamGraphForTestSource(
            WatermarkDeclaration watermarkDeclaration,
            Map<Integer, Watermark> subTaskId2needEmittedWatermarkInSourceOperator1)
            throws ReflectiveOperationException {
        ExecutionEnvironmentImpl env =
                (ExecutionEnvironmentImpl) ExecutionEnvironment.getInstance();
        env.getConfiguration().set(PipelineOptions.OPERATOR_CHAINING, false);

        ProcessConfigurableAndNonKeyedPartitionStream<Long> source =
                env.fromSource(
                                DataStreamV2SourceUtils.wrapSource(
                                        new Operator1Source(
                                                0,
                                                NUMBER_KEYS - 1,
                                                watermarkDeclaration,
                                                subTaskId2needEmittedWatermarkInSourceOperator1)),
                                "Operator1")
                        .withParallelism(DEFAULT_PARALLELISM);

        source.shuffle()
                .process(new Operator2ProcessFunction(null, null))
                .withName("Operator2")
                .withParallelism(DEFAULT_PARALLELISM);
        return env.getStreamGraph();
    }

    public StreamGraph getStreamGraphForAlignedWatermark(
            Map<Integer, Boolean> subTaskId2needEmittedWatermarkValue)
            throws ReflectiveOperationException {
        // create aligned watermark declaration
        AlignableBoolWatermarkDeclaration watermarkDeclaration =
                new AlignableBoolWatermarkDeclaration(
                        DEFAULT_WATERMARK_IDENTIFIER,
                        new WatermarkCombinationPolicy(
                                WatermarkCombinationFunction.BoolWatermarkCombinationFunction.AND,
                                true),
                        WatermarkHandlingStrategy.FORWARD,
                        true);

        Map<Integer, Watermark> subTaskId2needEmittedWatermarkInOperator2 =
                Map.of(
                        0,
                        watermarkDeclaration.newWatermark(
                                subTaskId2needEmittedWatermarkValue.get(0)),
                        1,
                        watermarkDeclaration.newWatermark(
                                subTaskId2needEmittedWatermarkValue.get(1)));

        return getStreamGraphForTestcase(
                watermarkDeclaration,
                subTaskId2needEmittedWatermarkInOperator2,
                WatermarkHandlingResult.PEEK);
    }

    public StreamGraph getStreamGraphForLongWatermarkCombineFunction(
            boolean combineMax, Map<Integer, Long> subTaskId2needEmittedWatermarkValue)
            throws ReflectiveOperationException {
        // create long watermark declaration
        WatermarkDeclarations.WatermarkDeclarationBuilder.LongWatermarkDeclarationBuilder
                watermarkBuilder =
                        WatermarkDeclarations.newBuilder(DEFAULT_WATERMARK_IDENTIFIER).typeLong();
        if (combineMax) {
            watermarkBuilder.combineFunctionMax();
        } else {
            watermarkBuilder.combineFunctionMin();
        }
        LongWatermarkDeclaration watermarkDeclaration = watermarkBuilder.build();

        Map<Integer, Watermark> subTaskId2needEmittedWatermarkInOperator2 =
                Map.of(
                        0,
                        watermarkDeclaration.newWatermark(
                                subTaskId2needEmittedWatermarkValue.get(0)),
                        1,
                        watermarkDeclaration.newWatermark(
                                subTaskId2needEmittedWatermarkValue.get(1)));

        return getStreamGraphForTestcase(
                watermarkDeclaration,
                subTaskId2needEmittedWatermarkInOperator2,
                WatermarkHandlingResult.PEEK);
    }

    public StreamGraph getStreamGraphForBoolWatermarkCombineFunction(
            boolean combineAnd, Map<Integer, Boolean> subTaskId2needEmittedWatermarkValue)
            throws ReflectiveOperationException {
        // create bool watermark declaration
        WatermarkDeclarations.WatermarkDeclarationBuilder.BoolWatermarkDeclarationBuilder
                watermarkBuilder =
                        WatermarkDeclarations.newBuilder(DEFAULT_WATERMARK_IDENTIFIER).typeBool();
        if (combineAnd) {
            watermarkBuilder.combineFunctionAND();
        } else {
            watermarkBuilder.combineFunctionOR();
        }
        BoolWatermarkDeclaration watermarkDeclaration = watermarkBuilder.build();

        Map<Integer, Watermark> subTaskId2needEmittedWatermarkInOperator2 =
                Map.of(
                        0,
                        watermarkDeclaration.newWatermark(
                                subTaskId2needEmittedWatermarkValue.get(0)),
                        1,
                        watermarkDeclaration.newWatermark(
                                subTaskId2needEmittedWatermarkValue.get(1)));

        return getStreamGraphForTestcase(
                watermarkDeclaration,
                subTaskId2needEmittedWatermarkInOperator2,
                WatermarkHandlingResult.PEEK);
    }

    public StreamGraph getStreamGraphForCombineWaitForAllChannels(
            Map<Integer, Long> subTaskId2needEmittedWatermarkValue)
            throws ReflectiveOperationException {
        // create long watermark declaration with combineWaitForAllChannels
        LongWatermarkDeclaration watermarkDeclaration =
                WatermarkDeclarations.newBuilder(DEFAULT_WATERMARK_IDENTIFIER)
                        .typeLong()
                        .combineFunctionMax()
                        .combineWaitForAllChannels(true)
                        .build();

        Map<Integer, Watermark> subTaskId2needEmittedWatermarkInOperator2 =
                Map.of(
                        0,
                        watermarkDeclaration.newWatermark(
                                subTaskId2needEmittedWatermarkValue.get(0)),
                        1,
                        watermarkDeclaration.newWatermark(
                                subTaskId2needEmittedWatermarkValue.get(1)));

        return getStreamGraphForTestcase(
                watermarkDeclaration,
                subTaskId2needEmittedWatermarkInOperator2,
                WatermarkHandlingResult.PEEK);
    }

    public StreamGraph getStreamGraphForDefaultHandlingStrategyIgnore(
            Map<Integer, Long> subTaskId2needEmittedWatermarkValue)
            throws ReflectiveOperationException {
        // create long watermark declaration with WatermarkHandlingStrategy.IGNORE
        LongWatermarkDeclaration watermarkDeclaration =
                WatermarkDeclarations.newBuilder(DEFAULT_WATERMARK_IDENTIFIER)
                        .typeLong()
                        .combineFunctionMax()
                        .defaultHandlingStrategyIgnore()
                        .build();

        Map<Integer, Watermark> subTaskId2needEmittedWatermarkInOperator2 =
                Map.of(
                        0,
                        watermarkDeclaration.newWatermark(
                                subTaskId2needEmittedWatermarkValue.get(0)),
                        1,
                        watermarkDeclaration.newWatermark(
                                subTaskId2needEmittedWatermarkValue.get(1)));

        return getStreamGraphForTestcase(
                watermarkDeclaration,
                subTaskId2needEmittedWatermarkInOperator2,
                WatermarkHandlingResult.PEEK);
    }

    public StreamGraph getStreamGraphForWatermarkHandlingResultIsPoll(
            Map<Integer, Long> subTaskId2needEmittedWatermarkValue)
            throws ReflectiveOperationException {
        // create watermark declaration, and make user process watermark function return
        // WatermarkHandlingResult.POLL
        LongWatermarkDeclaration watermarkDeclaration =
                WatermarkDeclarations.newBuilder(DEFAULT_WATERMARK_IDENTIFIER)
                        .typeLong()
                        .combineFunctionMax()
                        .build();

        Map<Integer, Watermark> subTaskId2needEmittedWatermarkInOperator2 =
                Map.of(
                        0,
                        watermarkDeclaration.newWatermark(
                                subTaskId2needEmittedWatermarkValue.get(0)),
                        1,
                        watermarkDeclaration.newWatermark(
                                subTaskId2needEmittedWatermarkValue.get(1)));

        return getStreamGraphForTestcase(
                watermarkDeclaration,
                subTaskId2needEmittedWatermarkInOperator2,
                WatermarkHandlingResult.POLL);
    }

    public StreamGraph getStreamGraphForSourceDeclareAndEmitWatermarks(
            Map<Integer, Long> subTaskId2needEmittedWatermarkValue)
            throws ReflectiveOperationException {
        // create watermark declaration
        LongWatermarkDeclaration watermarkDeclaration =
                WatermarkDeclarations.newBuilder(DEFAULT_WATERMARK_IDENTIFIER)
                        .typeLong()
                        .combineFunctionMax()
                        .build();

        Map<Integer, Watermark> emitWatermarksInSourceOperator1 =
                Map.of(
                        0,
                        watermarkDeclaration.newWatermark(
                                subTaskId2needEmittedWatermarkValue.get(0)),
                        1,
                        watermarkDeclaration.newWatermark(
                                subTaskId2needEmittedWatermarkValue.get(1)));

        return getStreamGraphForTestSource(watermarkDeclaration, emitWatermarksInSourceOperator1);
    }

    private static void tryWaitUntilCondition(SupplierWithException<Boolean, Exception> condition) {
        try {
            CommonTestUtils.waitUntilCondition(condition);
        } catch (Exception exception) {
        }
    }

    private static void setSubtaskBlocked(
            List<Integer> indices, boolean block, Map<Integer, Boolean> subtaskBlocked) {
        indices.forEach(index -> subtaskBlocked.put(index, block));
    }

    private static void checkSinkResults() {
        List<Long> actualResults = new ArrayList<>(Operator4ProcessFunction.receivedRecords);
        List<Long> expectedResults =
                LongStream.range(0, NUMBER_KEYS)
                        .map(x -> x * 2 + 1)
                        .boxed()
                        .collect(Collectors.toList());

        Collections.sort(actualResults);
        Collections.sort(expectedResults);
        assertThat(actualResults).isEqualTo(expectedResults);
    }

    private static <T> void assertOperatorReceivedWatermarkValues(
            Map<Integer, List<Watermark>> receivedWatermarks,
            boolean watermarkIsLongType,
            T... shouldContainedWatermarkValuesPerTask) {
        for (int i = 0; i < receivedWatermarks.size(); i++) {
            List<Watermark> watermarks = receivedWatermarks.get(i);
            assertThat(watermarks.size()).isEqualTo(shouldContainedWatermarkValuesPerTask.length);
            for (int j = 0; j < watermarks.size(); j++) {
                if (watermarkIsLongType) {
                    assertThat(watermarks.get(j)).isInstanceOf(LongWatermark.class);
                    assertThat(((LongWatermark) watermarks.get(j)).getValue())
                            .isEqualTo(shouldContainedWatermarkValuesPerTask[j]);
                } else {
                    assertThat(watermarks.get(j)).isInstanceOf(BoolWatermark.class);
                    assertThat(((BoolWatermark) watermarks.get(j)).getValue())
                            .isEqualTo(shouldContainedWatermarkValuesPerTask[j]);
                }
            }
        }
    }

    private static <T> boolean checkOperatorReceivedWatermarksAllNotEmpty(
            Map<Integer, List<Watermark>> receivedWatermarks) {
        if (receivedWatermarks.isEmpty()) {
            return false;
        }

        for (int i = 0; i < receivedWatermarks.size(); i++) {
            if (receivedWatermarks.get(i).isEmpty()) {
                return false;
            }
        }
        return true;
    }
}
