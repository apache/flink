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

package org.apache.flink.test.checkpointing.utils;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/** Test utilities for rescaling. */
public class RescalingTestUtils {

    /** A parallel source with definite keys. */
    public static class DefiniteKeySource extends RichParallelSourceFunction<Integer> {

        private static final long serialVersionUID = -400066323594122516L;

        private final int numberKeys;
        private final int numberElements;
        private final boolean terminateAfterEmission;

        protected int counter = 0;

        private boolean running = true;

        public DefiniteKeySource(
                int numberKeys, int numberElements, boolean terminateAfterEmission) {
            this.numberKeys = numberKeys;
            this.numberElements = numberElements;
            this.terminateAfterEmission = terminateAfterEmission;
        }

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            final Object lock = ctx.getCheckpointLock();
            final int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
            while (running) {

                if (counter < numberElements) {
                    synchronized (lock) {
                        for (int value = subtaskIndex;
                                value < numberKeys;
                                value += getRuntimeContext().getNumberOfParallelSubtasks()) {
                            ctx.collect(value);
                        }
                        counter++;
                    }
                } else {
                    if (terminateAfterEmission) {
                        running = false;
                    } else {
                        Thread.sleep(100);
                    }
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    /** A flatMapper with the index of subtask. */
    public static class SubtaskIndexFlatMapper
            extends RichFlatMapFunction<Integer, Tuple2<Integer, Integer>>
            implements CheckpointedFunction {

        private static final long serialVersionUID = 5273172591283191348L;

        public static CountDownLatch workCompletedLatch = new CountDownLatch(1);

        private transient ValueState<Integer> counter;
        private transient ValueState<Integer> sum;

        private final int numberElements;

        public SubtaskIndexFlatMapper(int numberElements) {
            this.numberElements = numberElements;
        }

        @Override
        public void flatMap(Integer value, Collector<Tuple2<Integer, Integer>> out)
                throws Exception {

            int count = counter.value() + 1;
            counter.update(count);

            int s = sum.value() + value;
            sum.update(s);

            if (count % numberElements == 0) {
                out.collect(Tuple2.of(getRuntimeContext().getIndexOfThisSubtask(), s));
                workCompletedLatch.countDown();
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            // all managed, nothing to do.
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            counter =
                    context.getKeyedStateStore()
                            .getState(new ValueStateDescriptor<>("counter", Integer.class, 0));
            sum =
                    context.getKeyedStateStore()
                            .getState(new ValueStateDescriptor<>("sum", Integer.class, 0));
        }
    }

    /** Sink for collecting results into a collection. */
    public static class CollectionSink<IN> implements SinkFunction<IN> {

        private static final Set<Object> elements =
                Collections.newSetFromMap(new ConcurrentHashMap<>());

        private static final long serialVersionUID = -1652452958040267745L;

        public static <IN> Set<IN> getElementsSet() {
            return (Set<IN>) elements;
        }

        public static void clearElementsSet() {
            elements.clear();
        }

        @Override
        public void invoke(IN value) throws Exception {
            elements.add(value);
        }
    }
}
