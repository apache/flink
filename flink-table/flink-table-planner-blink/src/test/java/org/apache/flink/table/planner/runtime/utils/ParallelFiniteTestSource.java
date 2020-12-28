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

package org.apache.flink.table.planner.runtime.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.util.FiniteTestSource;

import java.util.Iterator;

/** Parallel {@link FiniteTestSource} version. */
public class ParallelFiniteTestSource<T> extends RichSourceFunction<T>
        implements CheckpointListener, ParallelSourceFunction<T> {

    private final Iterable<T> elements;

    private transient volatile boolean running;
    private transient volatile long currentCheckpointId;

    public ParallelFiniteTestSource(Iterable<T> elements) {
        this.elements = elements;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        running = true;
        currentCheckpointId = 0;
    }

    public boolean isTaskMessage(int id) {
        return id % getRuntimeContext().getNumberOfParallelSubtasks()
                == getRuntimeContext().getIndexOfThisSubtask();
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        // first round of sending the elements and waiting for the checkpoints
        emitElementsAndWaitForCheckpoints(ctx, 2);

        // second round of the same
        emitElementsAndWaitForCheckpoints(ctx, 4);
    }

    private void emitElementsAndWaitForCheckpoints(SourceContext<T> ctx, long checkpointIdToWaitFor)
            throws InterruptedException {
        final Object lock = ctx.getCheckpointLock();

        synchronized (lock) {
            emitRecords(ctx);

            while (running && currentCheckpointId < checkpointIdToWaitFor) {
                lock.wait(1);
            }
        }
    }

    private void emitRecords(SourceContext<T> ctx) {
        Iterator<T> iterator = elements.iterator();
        int i = 0;
        while (iterator.hasNext()) {
            T next = iterator.next();
            if (isTaskMessage(i)) {
                ctx.collect(next);
            }
            i++;
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        currentCheckpointId = checkpointId;
    }
}
