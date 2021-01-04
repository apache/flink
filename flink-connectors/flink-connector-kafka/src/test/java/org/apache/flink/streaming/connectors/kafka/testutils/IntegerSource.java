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

package org.apache.flink.streaming.connectors.kafka.testutils;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.SerializableObject;

import java.util.Collections;
import java.util.List;

/**
 * A Flink source that servers integers, but it completes only after a completed checkpoint after
 * serving all of the elements.
 */
public class IntegerSource extends RichParallelSourceFunction<Integer>
        implements ListCheckpointed<Integer>, CheckpointListener {

    /**
     * Blocker when the generator needs to wait for the checkpoint to happen. Eager initialization
     * means it must be serializable (pick any serializable type).
     */
    private final Object blocker = new SerializableObject();

    /** The total number of events to generate. */
    private final int numEventsTotal;

    /** The current position in the sequence of numbers. */
    private int currentPosition = -1;

    private long lastCheckpointTriggered;

    private long lastCheckpointConfirmed;

    private boolean restored;

    private volatile boolean running = true;

    public IntegerSource(int numEventsTotal) {
        this.numEventsTotal = numEventsTotal;
    }

    @Override
    public void run(SourceContext<Integer> ctx) throws Exception {

        // each source subtask emits only the numbers where (num % parallelism == subtask_index)
        final int stepSize = getRuntimeContext().getNumberOfParallelSubtasks();
        int current =
                this.currentPosition >= 0
                        ? this.currentPosition
                        : getRuntimeContext().getIndexOfThisSubtask();

        while (this.running && current < this.numEventsTotal) {
            // emit the next element
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(current);
                current += stepSize;
                this.currentPosition = current;
            }
            // give some time to trigger checkpoint while we are not holding the lock (to prevent
            // starvation)
            if (!restored && current % 10 == 0) {
                Thread.sleep(1);
            }
        }

        // after we are done, we need to wait for two more checkpoint to complete
        // before finishing the program - that is to be on the safe side that
        // the sink also got the "commit" notification for all relevant checkpoints
        // and committed the data
        final long lastCheckpoint;
        synchronized (ctx.getCheckpointLock()) {
            lastCheckpoint = this.lastCheckpointTriggered;
        }

        synchronized (this.blocker) {
            while (this.lastCheckpointConfirmed <= lastCheckpoint + 1) {
                this.blocker.wait();
            }
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }

    @Override
    public List<Integer> snapshotState(long checkpointId, long checkpointTimestamp)
            throws Exception {
        this.lastCheckpointTriggered = checkpointId;

        return Collections.singletonList(this.currentPosition);
    }

    @Override
    public void restoreState(List<Integer> state) throws Exception {
        this.currentPosition = state.get(0);

        // at least one checkpoint must have happened so far
        this.lastCheckpointTriggered = 1L;
        this.lastCheckpointConfirmed = 1L;
        this.restored = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        synchronized (blocker) {
            this.lastCheckpointConfirmed = checkpointId;
            blocker.notifyAll();
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {}
}
