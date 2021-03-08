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

package org.apache.flink.connector.jdbc.xa;

import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.Serializable;
import java.util.List;

public class CheckpointAwaitingSource<T extends Serializable>
        implements SourceFunction<T>, CheckpointListener, CheckpointedFunction {
    private volatile boolean allDataEmitted = false;
    private volatile boolean dataCheckpointed = false;
    private volatile boolean running = true;
    private volatile long checkpointAfterData = -1L;
    private final List<T> data;

    public CheckpointAwaitingSource(List<T> data) {
        this.data = data;
    }

    @Override
    public void run(SourceContext<T> ctx) {
        for (T datum : data) {
            ctx.collect(datum);
        }
        allDataEmitted = true;
        while (!dataCheckpointed && running) {
            Thread.yield();
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        if (checkpointId == this.checkpointAfterData) {
            dataCheckpointed = true;
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) {
        if (allDataEmitted) {
            checkpointAfterData = context.getCheckpointId();
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) {}
}
