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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.concurrent.FutureUtils.assertNoException;

/** Input processor for {@link MultipleInputStreamOperator}. */
@Internal
public final class StreamMultipleInputProcessor implements StreamInputProcessor {

    private final MultipleInputSelectionHandler inputSelectionHandler;

    private final StreamOneInputProcessor<?>[] inputProcessors;

    private final MultipleInputAvailabilityHelper availabilityHelper;
    /** Always try to read from the first input. */
    private int lastReadInputIndex = 1;

    private boolean isPrepared;

    public StreamMultipleInputProcessor(
            MultipleInputSelectionHandler inputSelectionHandler,
            StreamOneInputProcessor<?>[] inputProcessors) {
        this.inputSelectionHandler = inputSelectionHandler;
        this.inputProcessors = inputProcessors;
        this.availabilityHelper = new MultipleInputAvailabilityHelper(inputProcessors.length);
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        if (inputSelectionHandler.isAnyInputAvailable()
                || inputSelectionHandler.areAllInputsFinished()) {
            return AVAILABLE;
        }

        availabilityHelper.resetToUnAvailable();
        for (int i = 0; i < inputProcessors.length; i++) {
            if (!inputSelectionHandler.isInputFinished(i)
                    && inputSelectionHandler.isInputSelected(i)) {
                availabilityHelper.anyOf(i, inputProcessors[i].getAvailableFuture());
            }
        }
        return availabilityHelper.getAvailableFuture();
    }

    @Override
    public DataInputStatus processInput() throws Exception {
        int readingInputIndex;
        if (isPrepared) {
            readingInputIndex = selectNextReadingInputIndex();
        } else {
            // the preparations here are not placed in the constructor because all work in it
            // must be executed after all operators are opened.
            readingInputIndex = selectFirstReadingInputIndex();
        }
        if (readingInputIndex == InputSelection.NONE_AVAILABLE) {
            return DataInputStatus.NOTHING_AVAILABLE;
        }

        lastReadInputIndex = readingInputIndex;
        DataInputStatus inputStatus = inputProcessors[readingInputIndex].processInput();
        return inputSelectionHandler.updateStatusAndSelection(inputStatus, readingInputIndex);
    }

    private int selectFirstReadingInputIndex() {
        // Note: the first call to nextSelection () on the operator must be made after this operator
        // is opened to ensure that any changes about the input selection in its open()
        // method take effect.
        inputSelectionHandler.nextSelection();

        isPrepared = true;

        return selectNextReadingInputIndex();
    }

    @Override
    public void close() throws IOException {
        IOException ex = null;
        for (StreamOneInputProcessor<?> input : inputProcessors) {
            try {
                input.close();
            } catch (IOException e) {
                ex = ExceptionUtils.firstOrSuppressed(e, ex);
            }
        }

        if (ex != null) {
            throw ex;
        }
    }

    @Override
    public CompletableFuture<Void> prepareSnapshot(
            ChannelStateWriter channelStateWriter, long checkpointId) throws CheckpointException {
        CompletableFuture<?>[] inputFutures = new CompletableFuture[inputProcessors.length];
        for (int index = 0; index < inputFutures.length; index++) {
            inputFutures[index] =
                    inputProcessors[index].prepareSnapshot(channelStateWriter, checkpointId);
        }
        return CompletableFuture.allOf(inputFutures);
    }

    private int selectNextReadingInputIndex() {
        if (!inputSelectionHandler.isAnyInputAvailable()) {
            fullCheckAndSetAvailable();
        }

        int readingInputIndex = inputSelectionHandler.selectNextInputIndex(lastReadInputIndex);
        if (readingInputIndex == InputSelection.NONE_AVAILABLE) {
            return InputSelection.NONE_AVAILABLE;
        }

        // to avoid starvation, if the input selection is ALL and availableInputsMask is not ALL,
        // always try to check and set the availability of another input
        if (inputSelectionHandler.shouldSetAvailableForAnotherInput()) {
            fullCheckAndSetAvailable();
        }

        return readingInputIndex;
    }

    private void fullCheckAndSetAvailable() {
        for (int i = 0; i < inputProcessors.length; i++) {
            StreamOneInputProcessor<?> inputProcessor = inputProcessors[i];
            // TODO: isAvailable() can be a costly operation (checking volatile). If one of
            // the input is constantly available and another is not, we will be checking this
            // volatile
            // once per every record. This might be optimized to only check once per processed
            // NetworkBuffer
            if (inputProcessor.isApproximatelyAvailable() || inputProcessor.isAvailable()) {
                inputSelectionHandler.setAvailableInput(i);
            }
        }
    }

    /**
     * This class is semi-thread safe. Only method {@link #notifyCompletion()} is allowed to be
     * executed from an outside of the task thread.
     *
     * <p>It solves a problem of a potential memory leak as described in FLINK-25728. In short we
     * have to ensure, that if there is one input (future) that rarely (or never) completes, that
     * such future would not prevent previously returned combined futures (like {@link
     * CompletableFuture#anyOf(CompletableFuture[])} from being garbage collected. Additionally, we
     * don't want to accumulate more and more completion stages on such rarely completed future, so
     * we are registering {@link CompletableFuture#thenRun(Runnable)} only if it has not already
     * been done.
     *
     * <p>Note {@link #resetToUnAvailable()} doesn't de register previously registered futures. If
     * future was registered in the past, but for whatever reason now it is not, such future can
     * still complete the newly created future.
     *
     * <p>It might be no longer needed after upgrading to JDK9
     * (https://bugs.openjdk.java.net/browse/JDK-8160402).
     */
    private static class MultipleInputAvailabilityHelper {
        private final CompletableFuture<?>[] futuresToCombine;

        private volatile CompletableFuture<?> availableFuture = new CompletableFuture<>();

        public MultipleInputAvailabilityHelper(int inputSize) {
            futuresToCombine = new CompletableFuture[inputSize];
        }

        /** @return combined future using anyOf logic */
        public CompletableFuture<?> getAvailableFuture() {
            return availableFuture;
        }

        public void resetToUnAvailable() {
            if (availableFuture.isDone()) {
                availableFuture = new CompletableFuture<>();
            }
        }

        private void notifyCompletion() {
            availableFuture.complete(null);
        }

        /**
         * Combine {@code availabilityFuture} using anyOf logic with other previously registered
         * futures.
         */
        public void anyOf(final int idx, CompletableFuture<?> availabilityFuture) {
            if (futuresToCombine[idx] == null || futuresToCombine[idx].isDone()) {
                futuresToCombine[idx] = availabilityFuture;
                assertNoException(availabilityFuture.thenRun(this::notifyCompletion));
            }
        }
    }
}
