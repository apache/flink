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

package org.apache.flink.connector.datagen.source;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceReaderBase;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceSplit;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link SourceReader} that synchronizes emission of N elements on the arrival of the checkpoint
 * barriers It 1) emits a list of elements without checkpoints in-between, 2) then waits for two
 * checkpoints to complete, 3) then re-emits the same elements before 4) waiting for another two
 * checkpoints and 5) exiting.
 *
 * <p>This lockstep execution is possible because {@code pollNext} and {@code snapshotState} are
 * executed in the same thread and the fact that {@code pollNext} can emit N elements at once. This
 * reader is meant to be used solely for testing purposes as the substitution for the {@code
 * FiniteTestSource} which implements the deprecated {@code SourceFunction} API.
 */
@Experimental
public class DoubleEmittingSourceReaderWithCheckpointsInBetween<
                E, O, IterT extends Iterator<E>, SplitT extends IteratorSourceSplit<E, IterT>>
        extends IteratorSourceReaderBase<E, O, IterT, SplitT> {

    private final GeneratorFunction<E, O> generatorFunction;

    private BooleanSupplier allowedToExit;
    private int snapshotsCompleted;
    private int snapshotsToWaitFor;
    private boolean done;

    public DoubleEmittingSourceReaderWithCheckpointsInBetween(
            SourceReaderContext context,
            GeneratorFunction<E, O> generatorFunction,
            @Nullable BooleanSupplier allowedToExit) {
        super(context);
        this.generatorFunction = checkNotNull(generatorFunction);
        this.allowedToExit = allowedToExit;
    }

    public DoubleEmittingSourceReaderWithCheckpointsInBetween(
            SourceReaderContext context, GeneratorFunction<E, O> generatorFunction) {
        super(context);
        this.generatorFunction = checkNotNull(generatorFunction);
    }

    // ------------------------------------------------------------------------

    @Override
    public void start(SourceReaderContext context) {
        try {
            generatorFunction.open(context);
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to open the GeneratorFunction", e);
        }
    }

    @Override
    public InputStatus pollNext(ReaderOutput<O> output) {
        // This is the termination path after the test data has been emitted twice
        if (done) {
            if (allowedToExit != null) { // Termination is controlled externally
                return allowedToExit.getAsBoolean()
                        ? InputStatus.END_OF_INPUT
                        : InputStatus.NOTHING_AVAILABLE;
            } else {
                return InputStatus.END_OF_INPUT;
            }
        }
        // This is the initial path
        if (currentSplit == null) {
            InputStatus inputStatus = tryMoveToNextSplit();
            switch (inputStatus) {
                case MORE_AVAILABLE:
                    emitElements(output);
                    break;
                case END_OF_INPUT:
                    // This can happen if the source parallelism is larger than the number of
                    // available splits
                    return inputStatus;
            }
        } else {
            // This is the path that emits the same split the second time
            emitElements(output);
            done = true;
        }
        availability = new CompletableFuture<>();
        return InputStatus.NOTHING_AVAILABLE;
    }

    private void emitElements(ReaderOutput<O> output) {
        iterator = currentSplit.getIterator();
        while (iterator.hasNext()) {
            E next = iterator.next();
            O converted = convert(next);
            output.collect(converted);
        }
        // Always wait for two snapshots after emitting the elements
        snapshotsToWaitFor = 2;
        snapshotsCompleted = 0;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        snapshotsCompleted++;
        if (snapshotsCompleted >= snapshotsToWaitFor) {
            availability.complete(null);
        }

        if (allowedToExit != null) {
            if (allowedToExit.getAsBoolean()) {
                availability.complete(null);
            }
        }
    }

    @Override
    protected O convert(E value) {
        try {
            return generatorFunction.map(value);
        } catch (Exception e) {
            String message =
                    String.format(
                            "A user-provided generator function threw an exception on this input: %s",
                            value.toString());
            throw new FlinkRuntimeException(message, e);
        }
    }
}
