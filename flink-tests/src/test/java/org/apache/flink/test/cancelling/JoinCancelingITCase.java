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

package org.apache.flink.test.cancelling;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.test.util.InfiniteIntegerTupleInputFormat;
import org.apache.flink.test.util.UniformIntTupleGeneratorInputFormat;

import org.junit.Ignore;
import org.junit.Test;

/** Test job cancellation from within a JoinFunction. */
@Ignore("Takes too long.")
public class JoinCancelingITCase extends CancelingTestBase {

    // --------------- Test Sort Matches that are canceled while still reading / sorting
    // -----------------
    private void executeTask(
            JoinFunction<
                            Tuple2<Integer, Integer>,
                            Tuple2<Integer, Integer>,
                            Tuple2<Integer, Integer>>
                    joiner,
            boolean slow)
            throws Exception {
        executeTask(joiner, slow, PARALLELISM);
    }

    private void executeTask(
            JoinFunction<
                            Tuple2<Integer, Integer>,
                            Tuple2<Integer, Integer>,
                            Tuple2<Integer, Integer>>
                    joiner,
            boolean slow,
            int parallelism)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<Integer, Integer>> input1 =
                env.createInput(new InfiniteIntegerTupleInputFormat(slow));
        DataStreamSource<Tuple2<Integer, Integer>> input2 =
                env.createInput(new InfiniteIntegerTupleInputFormat(slow));

        input1.join(input2)
                .where(value -> value.f0)
                .equalTo(value -> value.f0)
                .window(GlobalWindows.createWithEndOfStreamTrigger())
                .apply(joiner)
                .sinkTo(new DiscardingSink<>());

        env.setParallelism(parallelism);

        runAndCancelJob(env.getStreamGraph().getJobGraph(), 5 * 1000, 10 * 1000);
    }

    @Test
    public void testCancelWhileReadingSlowInputs() throws Exception {
        executeTask(new SimpleMatcher<Integer>(), true);
    }

    @Test
    public void testCancelWhileReadingFastInputs() throws Exception {
        executeTask(new SimpleMatcher<Integer>(), false);
    }

    @Test
    public void testCancelPriorToFirstRecordReading() throws Exception {
        executeTask(new StuckInOpenMatcher<Integer>(), false);
    }

    private void executeTaskWithGenerator(
            JoinFunction<
                            Tuple2<Integer, Integer>,
                            Tuple2<Integer, Integer>,
                            Tuple2<Integer, Integer>>
                    joiner,
            int keys,
            int vals,
            int msecsTillCanceling,
            int maxTimeTillCanceled)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<Integer, Integer>> input1 =
                env.createInput(new UniformIntTupleGeneratorInputFormat(keys, vals));
        DataStreamSource<Tuple2<Integer, Integer>> input2 =
                env.createInput(new UniformIntTupleGeneratorInputFormat(keys, vals));

        input1.join(input2)
                .where(value -> value.f0)
                .equalTo(value -> value.f0)
                .window(GlobalWindows.createWithEndOfStreamTrigger())
                .apply(joiner)
                .sinkTo(new DiscardingSink<>());

        runAndCancelJob(
                env.getStreamGraph().getJobGraph(), msecsTillCanceling, maxTimeTillCanceled);
    }

    // --------------- Test Sort Matches that are canceled while in the Matching Phase
    // -----------------

    @Test
    public void testCancelWhileJoining() throws Exception {
        executeTaskWithGenerator(new DelayingMatcher<Integer>(), 500, 3, 10 * 1000, 20 * 1000);
    }

    @Test
    public void testCancelWithLongCancellingResponse() throws Exception {
        executeTaskWithGenerator(
                new LongCancelTimeMatcher<Integer>(), 500, 3, 10 * 1000, 10 * 1000);
    }

    // -------------------------------------- Test System corner cases
    // ---------------------------------

    @Test
    public void testCancelWithHighparallelism() throws Exception {
        executeTask(new SimpleMatcher<Integer>(), false, 64);
    }

    // --------------------------------------------------------------------------------------------

    private static final class SimpleMatcher<IN>
            implements JoinFunction<Tuple2<IN, IN>, Tuple2<IN, IN>, Tuple2<IN, IN>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple2<IN, IN> join(Tuple2<IN, IN> first, Tuple2<IN, IN> second) throws Exception {
            return new Tuple2<>(first.f0, second.f0);
        }
    }

    private static final class DelayingMatcher<IN>
            implements JoinFunction<Tuple2<IN, IN>, Tuple2<IN, IN>, Tuple2<IN, IN>> {
        private static final long serialVersionUID = 1L;

        private static final int WAIT_TIME_PER_RECORD = 10 * 1000; // 10 sec.

        @Override
        public Tuple2<IN, IN> join(Tuple2<IN, IN> first, Tuple2<IN, IN> second) throws Exception {
            Thread.sleep(WAIT_TIME_PER_RECORD);
            return new Tuple2<>(first.f0, second.f0);
        }
    }

    private static final class LongCancelTimeMatcher<IN>
            implements JoinFunction<Tuple2<IN, IN>, Tuple2<IN, IN>, Tuple2<IN, IN>> {
        private static final long serialVersionUID = 1L;

        private static final int WAIT_TIME_PER_RECORD = 5 * 1000; // 5 sec.

        @Override
        public Tuple2<IN, IN> join(Tuple2<IN, IN> first, Tuple2<IN, IN> second) throws Exception {
            final long start = System.currentTimeMillis();
            long remaining = WAIT_TIME_PER_RECORD;
            do {
                try {
                    Thread.sleep(remaining);
                } catch (InterruptedException iex) {
                }
            } while ((remaining = WAIT_TIME_PER_RECORD - System.currentTimeMillis() + start) > 0);
            return new Tuple2<>(first.f0, second.f0);
        }
    }

    private static final class StuckInOpenMatcher<IN>
            extends RichJoinFunction<Tuple2<IN, IN>, Tuple2<IN, IN>, Tuple2<IN, IN>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void open(OpenContext openContext) throws Exception {
            synchronized (this) {
                wait();
            }
        }

        @Override
        public Tuple2<IN, IN> join(Tuple2<IN, IN> first, Tuple2<IN, IN> second) throws Exception {
            return new Tuple2<>(first.f0, second.f0);
        }
    }
}
