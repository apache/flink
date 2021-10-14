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

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Preconditions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * The FailingCollectionSource will fail after emitted a specified number of elements. This is used
 * to perform checkpoint and restore in IT cases.
 */
public class FailingCollectionSource<T>
        implements SourceFunction<T>, CheckpointedFunction, CheckpointListener {

    public static volatile boolean failedBefore = false;

    private static final long serialVersionUID = 1L;

    /** The (de)serializer to be used for the data elements. */
    private final TypeSerializer<T> serializer;

    /** The actual data elements, in serialized form. */
    private final byte[] elementsSerialized;

    /** The number of serialized elements. */
    private final int numElements;

    /** The number of elements emitted already. */
    private volatile int numElementsEmitted;

    /** The number of elements to skip initially. */
    private volatile int numElementsToSkip;

    /** Flag to make the source cancelable. */
    private volatile boolean isRunning = true;

    private transient ListState<Integer> checkpointedState;

    /** A failure will occur when the given number of elements have been processed. */
    private final int failureAfterNumElements;

    /** The number of completed checkpoints. */
    private volatile int numSuccessfulCheckpoints;

    /** The checkpointed number of emitted elements. */
    private final Map<Long, Integer> checkpointedEmittedNums;

    /** The last successful checkpointed number of emitted elements. */
    private volatile int lastCheckpointedEmittedNum = 0;

    public FailingCollectionSource(
            TypeSerializer<T> serializer, Iterable<T> elements, int failureAfterNumElements)
            throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(baos);

        int count = 0;
        try {
            for (T element : elements) {
                serializer.serialize(element, wrapper);
                count++;
            }
        } catch (Exception e) {
            throw new IOException("Serializing the source elements failed: " + e.getMessage(), e);
        }

        this.serializer = serializer;
        this.elementsSerialized = baos.toByteArray();
        this.numElements = count;
        checkArgument(count > 0, "testing elements of values source shouldn't be empty.");
        checkArgument(failureAfterNumElements > 0);
        this.failureAfterNumElements = failureAfterNumElements;
        this.checkpointedEmittedNums = new HashMap<>();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        Preconditions.checkState(
                this.checkpointedState == null,
                "The " + getClass().getSimpleName() + " has already been initialized.");

        this.checkpointedState =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>(
                                        "from-elements-state", IntSerializer.INSTANCE));

        if (context.isRestored()) {
            List<Integer> retrievedStates = new ArrayList<>();
            for (Integer entry : this.checkpointedState.get()) {
                retrievedStates.add(entry);
            }

            // given that the parallelism of the function is 1, we can only have 1 state
            Preconditions.checkArgument(
                    retrievedStates.size() == 1,
                    getClass().getSimpleName() + " retrieved invalid state.");

            this.numElementsToSkip = retrievedStates.get(0);
        }
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        ByteArrayInputStream bais = new ByteArrayInputStream(elementsSerialized);
        final DataInputView input = new DataInputViewStreamWrapper(bais);

        // if we are restored from a checkpoint and need to skip elements, skip them now.
        int toSkip = numElementsToSkip;
        if (toSkip > 0) {
            try {
                while (toSkip > 0) {
                    serializer.deserialize(input);
                    toSkip--;
                }
            } catch (Exception e) {
                throw new IOException(
                        "Failed to deserialize an element from the source. "
                                + "If you are using user-defined serialization (Value and Writable types), check the "
                                + "serialization functions.\nSerializer is "
                                + serializer);
            }

            this.numElementsEmitted = this.numElementsToSkip;
        }

        while (isRunning && numElementsEmitted < numElements) {
            if (!failedBefore) {
                // delay a bit, if we have not failed before
                Thread.sleep(1);
                if (numSuccessfulCheckpoints >= 1 && lastCheckpointedEmittedNum >= 1) {
                    // cause a failure if we have not failed before and have a completed checkpoint
                    // and have processed at least one element
                    failedBefore = true;
                    throw new Exception("Artificial Failure");
                }
            }

            if (failedBefore || numElementsEmitted < failureAfterNumElements) {
                // the function failed before, or we are in the elements before the failure
                T next;
                try {
                    next = serializer.deserialize(input);
                } catch (Exception e) {
                    throw new IOException(
                            "Failed to deserialize an element from the source. "
                                    + "If you are using user-defined serialization (Value and Writable types), check the "
                                    + "serialization functions.\nSerializer is "
                                    + serializer);
                }

                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(next);
                    numElementsEmitted++;
                }
            } else {
                // if our work is done, delay a bit to prevent busy waiting
                Thread.sleep(1);
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    /**
     * Gets the number of elements produced in total by this function.
     *
     * @return The number of elements produced in total.
     */
    public int getNumElements() {
        return numElements;
    }

    /**
     * Gets the number of elements emitted so far.
     *
     * @return The number of elements emitted so far.
     */
    public int getNumElementsEmitted() {
        return numElementsEmitted;
    }

    // ------------------------------------------------------------------------
    //  Checkpointing
    // ------------------------------------------------------------------------

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        Preconditions.checkState(
                this.checkpointedState != null,
                "The " + getClass().getSimpleName() + " has not been properly initialized.");

        this.checkpointedState.clear();
        this.checkpointedState.add(this.numElementsEmitted);
        long checkpointId = context.getCheckpointId();
        checkpointedEmittedNums.put(checkpointId, numElementsEmitted);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        numSuccessfulCheckpoints++;
        lastCheckpointedEmittedNum = checkpointedEmittedNums.get(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {}

    public static void reset() {
        failedBefore = false;
    }
}
