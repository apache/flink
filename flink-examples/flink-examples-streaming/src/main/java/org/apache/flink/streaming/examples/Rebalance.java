/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/** */
public class Rebalance {

    // *************************************************************************
    // PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(8);
        env.disableOperatorChaining();

        String rebalance = params.get("rebalance", "simple");

        DataStream<Long> stream = env.addSource(new LongSequence());

        stream = rebalance.equals("load") ? stream.loadRebalance() : stream.rebalance();

        stream.map(new SlowSubtaskMap()).addSink(new DiscardingSink<>());

        env.execute("Streaming Rebalance");
    }

    private static class LongSequence implements ParallelSourceFunction<Long> {
        volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            long next = 0;
            while (isRunning) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(next++);
                }
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    private static class SlowSubtaskMap extends RichMapFunction<Long, Long> {
        @Override
        public Long map(Long value) throws Exception {
            int ind = getRuntimeContext().getIndexOfThisSubtask();
            if (ind % 2 == 1) {
                Thread.sleep(1 * ind);
            }
            return value;
        }
    }
}
