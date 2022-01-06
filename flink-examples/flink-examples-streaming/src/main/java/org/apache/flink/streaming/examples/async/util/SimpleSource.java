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

package org.apache.flink.streaming.examples.async.util;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/** A checkpointed source. */
public class SimpleSource implements SourceFunction<Integer>, CheckpointedFunction {
    private static final long serialVersionUID = 1L;

    private volatile boolean isRunning = true;
    private int start = 0;

    private ListState<Integer> state;

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        state =
                context.getOperatorStateStore()
                        .getListState(new ListStateDescriptor<>("state", IntSerializer.INSTANCE));

        // restore any state that we might already have to our fields, initialize state
        // is also called in case of restore.
        for (Integer i : state.get()) {
            start = i;
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        state.clear();
        state.add(start);
    }

    @Override
    public void run(SourceContext<Integer> ctx) throws Exception {
        while (isRunning) {
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(start);
                ++start;

                // loop back to 0
                if (start == Integer.MAX_VALUE) {
                    start = 0;
                }
            }
            Thread.sleep(10L);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
