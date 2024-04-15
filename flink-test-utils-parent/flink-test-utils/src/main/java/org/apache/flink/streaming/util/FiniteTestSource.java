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

package org.apache.flink.streaming.util;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A stream source that: 1) emits a list of elements without allowing checkpoints, 2) then waits for
 * two more checkpoints to complete, 3) then re-emits the same elements before 4) waiting for
 * another two checkpoints and 5) exiting.
 *
 * <p>This class was written to test the Bulk Writers used by the StreamingFileSink.
 *
 * @deprecated This class is based on the {@link
 *     org.apache.flink.streaming.api.functions.source.SourceFunction} API, which is due to be
 *     removed. Use the new {@link org.apache.flink.api.connector.source.Source} API instead.
 */
@SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
@Deprecated
public class FiniteTestSource<T> implements SourceFunction<T>, CheckpointListener {

    private static final long serialVersionUID = 1L;

    @SuppressWarnings("NonSerializableFieldInSerializableClass")
    private final Iterable<T> elements;

    private volatile boolean running = true;

    private transient int numCheckpointsComplete;

    @Nullable private final BooleanSupplier couldExit;

    private final long waitTimeOut;

    @SafeVarargs
    public FiniteTestSource(T... elements) {
        this(null, 0, Arrays.asList(elements));
    }

    public FiniteTestSource(Iterable<T> elements) {
        this(null, 0, elements);
    }

    public FiniteTestSource(
            @Nullable BooleanSupplier couldExit, long waitTimeOut, Iterable<T> elements) {
        checkState(waitTimeOut >= 0);
        this.couldExit = couldExit;
        this.waitTimeOut = waitTimeOut;
        this.elements = elements;
    }

    public FiniteTestSource(@Nullable BooleanSupplier couldExit, Iterable<T> elements) {
        this.couldExit = couldExit;
        this.waitTimeOut = 30_000;
        this.elements = elements;
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        // first round of sending the elements and waiting for the checkpoints
        emitElementsAndWaitForCheckpoints(ctx, 2);

        // second round of the same
        emitElementsAndWaitForCheckpoints(ctx, 2);

        // verify the source could exit or not
        if (couldExit != null) {
            final long beginTime = System.currentTimeMillis();
            synchronized (ctx.getCheckpointLock()) {
                while (running && !couldExit.getAsBoolean()) {
                    ctx.getCheckpointLock().wait(10);
                    if ((System.currentTimeMillis() - beginTime) > waitTimeOut) {
                        throw new TimeoutException(
                                "Wait source exit time out " + waitTimeOut + "ms.");
                    }
                }
            }
        }
    }

    private void emitElementsAndWaitForCheckpoints(SourceContext<T> ctx, int checkpointsToWaitFor)
            throws InterruptedException {
        final Object lock = ctx.getCheckpointLock();

        final int checkpointToAwait;
        synchronized (lock) {
            checkpointToAwait = numCheckpointsComplete + checkpointsToWaitFor;
            for (T t : elements) {
                ctx.collect(t);
            }
        }

        synchronized (lock) {
            while (running && numCheckpointsComplete < checkpointToAwait) {
                lock.wait(1);
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        numCheckpointsComplete++;
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {}
}
