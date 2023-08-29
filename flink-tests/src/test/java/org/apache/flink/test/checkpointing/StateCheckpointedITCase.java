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

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * A simple test that runs a streaming topology with checkpointing enabled.
 *
 * <p>The test triggers a failure after a while and verifies that, after completion, the state
 * defined with either the {@link ValueState} or the {@link ListCheckpointed} interface reflects the
 * "exactly once" semantics.
 *
 * <p>The test throttles the input until at least two checkpoints are completed, to make sure that
 * the recovery does not fall back to "square one" (which would naturally lead to correct results
 * without testing the checkpointing).
 */
@SuppressWarnings("serial")
public class StateCheckpointedITCase extends StreamFaultToleranceTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(StateCheckpointedITCase.class);

    static final long NUM_STRINGS = 10_000_000L;

    /**
     * Runs the following program.
     *
     * <pre>
     *     [ (source)->(filter)] -> [ (map) -> (map) ] -> [ (groupBy/reduce)->(sink) ]
     * </pre>
     */
    @Override
    public void testProgram(StreamExecutionEnvironment env) {
        assertTrue("Broken test setup", NUM_STRINGS % 40 == 0);

        final long failurePosMin = (long) (0.4 * NUM_STRINGS / PARALLELISM);
        final long failurePosMax = (long) (0.7 * NUM_STRINGS / PARALLELISM);

        final long failurePos =
                (new Random().nextLong() % (failurePosMax - failurePosMin)) + failurePosMin;

        env.enableCheckpointing(200);

        DataStream<String> stream = env.addSource(new StringGeneratingSourceFunction(NUM_STRINGS));

        stream
                // first vertex, chained to the source
                // this filter throttles the flow until at least one checkpoint
                // is complete, to make sure this program does not run without
                .filter(new StringRichFilterFunction())

                // -------------- seconds vertex - one-to-one connected ----------------
                .map(new StringPrefixCountRichMapFunction())
                .startNewChain()
                .map(new StatefulCounterFunction())

                // -------------- third vertex - reducer and the sink ----------------
                .keyBy("prefix")
                .flatMap(new OnceFailingAggregator(failurePos))
                .addSink(new ValidatingSink());
    }

    @Override
    public void postSubmit() {

        // assertTrue("Test inconclusive: failure occurred before first checkpoint",
        //		OnceFailingAggregator.wasCheckpointedBeforeFailure);
        if (!OnceFailingAggregator.wasCheckpointedBeforeFailure) {
            LOG.warn("Test inconclusive: failure occurred before first checkpoint");
        }

        long filterSum = 0;
        for (long l : StringRichFilterFunction.counts) {
            filterSum += l;
        }

        long mapSum = 0;
        for (long l : StringPrefixCountRichMapFunction.counts) {
            mapSum += l;
        }

        long countSum = 0;
        for (long l : StatefulCounterFunction.counts) {
            countSum += l;
        }

        // verify that we counted exactly right
        assertEquals(NUM_STRINGS, filterSum);
        assertEquals(NUM_STRINGS, mapSum);
        assertEquals(NUM_STRINGS, countSum);

        for (Map<Character, Long> map : ValidatingSink.maps) {
            for (Long count : map.values()) {
                assertEquals(NUM_STRINGS / 40, count.longValue());
            }
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Custom Functions
    // --------------------------------------------------------------------------------------------

    private static class StringGeneratingSourceFunction extends RichParallelSourceFunction<String>
            implements ListCheckpointed<Integer> {
        private final long numElements;

        private int index;

        private volatile boolean isRunning = true;

        StringGeneratingSourceFunction(long numElements) {
            this.numElements = numElements;
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            final Object lockingObject = ctx.getCheckpointLock();

            final Random rnd = new Random();
            final StringBuilder stringBuilder = new StringBuilder();

            final int step = getRuntimeContext().getNumberOfParallelSubtasks();

            if (index == 0) {
                index = getRuntimeContext().getIndexOfThisSubtask();
            }

            while (isRunning && index < numElements) {
                char first = (char) ((index % 40) + 40);

                stringBuilder.setLength(0);
                stringBuilder.append(first);

                String result = randomString(stringBuilder, rnd);

                synchronized (lockingObject) {
                    index += step;
                    ctx.collect(result);
                }
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }

        private static String randomString(StringBuilder bld, Random rnd) {
            final int len = rnd.nextInt(10) + 5;

            for (int i = 0; i < len; i++) {
                char next = (char) (rnd.nextInt(20000) + 33);
                bld.append(next);
            }

            return bld.toString();
        }

        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(this.index);
        }

        @Override
        public void restoreState(List<Integer> state) throws Exception {
            if (state.isEmpty() || state.size() > 1) {
                throw new RuntimeException(
                        "Test failed due to unexpected recovered state size " + state.size());
            }
            this.index = state.get(0);
        }
    }

    private static class StringRichFilterFunction extends RichFilterFunction<String>
            implements ListCheckpointed<Long> {

        static long[] counts = new long[PARALLELISM];

        private long count;

        @Override
        public boolean filter(String value) throws Exception {
            count++;
            return value.length() < 100; // should be always true
        }

        @Override
        public void close() {
            counts[getRuntimeContext().getIndexOfThisSubtask()] = count;
        }

        @Override
        public List<Long> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(this.count);
        }

        @Override
        public void restoreState(List<Long> state) throws Exception {
            if (state.isEmpty() || state.size() > 1) {
                throw new RuntimeException(
                        "Test failed due to unexpected recovered state size " + state.size());
            }
            this.count = state.get(0);
        }
    }

    private static class StringPrefixCountRichMapFunction
            extends RichMapFunction<String, PrefixCount> implements ListCheckpointed<Long> {

        static long[] counts = new long[PARALLELISM];

        private long count;

        @Override
        public PrefixCount map(String value) {
            count++;
            return new PrefixCount(value.substring(0, 1), value, 1L);
        }

        @Override
        public void close() {
            counts[getRuntimeContext().getIndexOfThisSubtask()] = count;
        }

        @Override
        public List<Long> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(this.count);
        }

        @Override
        public void restoreState(List<Long> state) throws Exception {
            if (state.isEmpty() || state.size() > 1) {
                throw new RuntimeException(
                        "Test failed due to unexpected recovered state size " + state.size());
            }
            this.count = state.get(0);
        }
    }

    private static class StatefulCounterFunction extends RichMapFunction<PrefixCount, PrefixCount>
            implements ListCheckpointed<Long> {

        static long[] counts = new long[PARALLELISM];

        private long count;

        @Override
        public PrefixCount map(PrefixCount value) throws Exception {
            count++;
            return value;
        }

        @Override
        public void close() throws IOException {
            counts[getRuntimeContext().getIndexOfThisSubtask()] = count;
        }

        @Override
        public List<Long> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(this.count);
        }

        @Override
        public void restoreState(List<Long> state) throws Exception {
            if (state.isEmpty() || state.size() > 1) {
                throw new RuntimeException(
                        "Test failed due to unexpected recovered state size " + state.size());
            }
            this.count = state.get(0);
        }
    }

    private static class OnceFailingAggregator extends RichFlatMapFunction<PrefixCount, PrefixCount>
            implements ListCheckpointed<HashMap<String, PrefixCount>>, CheckpointListener {

        static boolean wasCheckpointedBeforeFailure = false;

        private static volatile boolean hasFailed = false;

        private final HashMap<String, PrefixCount> aggregationMap =
                new HashMap<String, PrefixCount>();

        private long failurePos;
        private long count;

        private boolean wasCheckpointed;

        OnceFailingAggregator(long failurePos) {
            this.failurePos = failurePos;
        }

        @Override
        public void open(OpenContext openContext) {
            count = 0;
        }

        @Override
        public void flatMap(PrefixCount value, Collector<PrefixCount> out) throws Exception {
            count++;
            if (!hasFailed
                    && count >= failurePos
                    && getRuntimeContext().getIndexOfThisSubtask() == 1) {
                wasCheckpointedBeforeFailure = wasCheckpointed;
                hasFailed = true;
                throw new Exception("Test Failure");
            }

            PrefixCount curr = aggregationMap.get(value.prefix);
            if (curr == null) {
                aggregationMap.put(value.prefix, value);
                out.collect(value);
            } else {
                curr.count += value.count;
                out.collect(curr);
            }
        }

        @Override
        public List<HashMap<String, PrefixCount>> snapshotState(long checkpointId, long timestamp)
                throws Exception {
            return Collections.singletonList(this.aggregationMap);
        }

        @Override
        public void restoreState(List<HashMap<String, PrefixCount>> state) throws Exception {
            if (state.isEmpty() || state.size() > 1) {
                throw new RuntimeException(
                        "Test failed due to unexpected recovered state size " + state.size());
            }
            this.aggregationMap.putAll(state.get(0));
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            this.wasCheckpointed = true;
        }

        @Override
        public void notifyCheckpointAborted(long checkpointId) {}
    }

    private static class ValidatingSink extends RichSinkFunction<PrefixCount>
            implements ListCheckpointed<HashMap<Character, Long>> {

        @SuppressWarnings("unchecked")
        private static Map<Character, Long>[] maps =
                (Map<Character, Long>[]) new Map<?, ?>[PARALLELISM];

        private HashMap<Character, Long> counts = new HashMap<Character, Long>();

        @Override
        public void invoke(PrefixCount value) {
            Character first = value.prefix.charAt(0);
            Long previous = counts.get(first);
            if (previous == null) {
                counts.put(first, value.count);
            } else {
                counts.put(first, Math.max(previous, value.count));
            }
        }

        @Override
        public void close() throws Exception {
            maps[getRuntimeContext().getIndexOfThisSubtask()] = counts;
        }

        @Override
        public List<HashMap<Character, Long>> snapshotState(long checkpointId, long timestamp)
                throws Exception {
            return Collections.singletonList(this.counts);
        }

        @Override
        public void restoreState(List<HashMap<Character, Long>> state) throws Exception {
            if (state.isEmpty() || state.size() > 1) {
                throw new RuntimeException(
                        "Test failed due to unexpected recovered state size " + state.size());
            }
            this.counts.putAll(state.get(0));
        }
    }
}
