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

package org.apache.flink.test.checkpointing.utils;

import org.apache.flink.api.common.accumulators.ListAccumulator;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A sink that emits received elements into an accumulator once the corresponding checkpoint is
 * completed.
 */
public class AccumulatingIntegerSink extends RichSinkFunction<Integer>
        implements CheckpointListener, CheckpointedFunction {
    private static final String ACCUMULATOR_NAME = "output";

    private List<Integer> current = new ArrayList<>();
    private final Map<Long, List<Integer>> pendingForAccumulator = new HashMap<>();
    private final ListAccumulator<Integer> accumulator = new ListAccumulator<>();
    private final int delayMillis;

    public AccumulatingIntegerSink(int delayMillis) {
        this.delayMillis = delayMillis;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        getRuntimeContext().addAccumulator(ACCUMULATOR_NAME, accumulator);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void invoke(Integer value, Context context) throws InterruptedException {
        current.add(value);
        if (delayMillis > 0) {
            Thread.sleep(delayMillis);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {}

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        pendingForAccumulator.put(context.getCheckpointId(), current);
        current = new ArrayList<>();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        pendingForAccumulator.remove(checkpointId).forEach(accumulator::add);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {}

    @SuppressWarnings("unchecked")
    public static List<Integer> getOutput(Map<String, Object> accumulators) {
        return (List<Integer>) accumulators.get(ACCUMULATOR_NAME);
    }
}
