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
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.concurrent.FutureUtils.assertNoException;

/** Input processor for {@link MultipleInputStreamOperator}. */
@Internal
public final class StreamMultipleInputProcessor implements StreamInputProcessor {

    private final MultipleInputSelectionHandler inputSelectionHandler;

    private final StreamOneInputProcessor<?>[] inputProcessors;
    /** Always try to read from the first input. */
    private int lastReadInputIndex = 1;

    private boolean isPrepared;

    public StreamMultipleInputProcessor(
            MultipleInputSelectionHandler inputSelectionHandler,
            StreamOneInputProcessor<?>[] inputProcessors) {
        this.inputSelectionHandler = inputSelectionHandler;
        this.inputProcessors = inputProcessors;
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        if (inputSelectionHandler.isAnyInputAvailable()
                || inputSelectionHandler.areAllInputsFinished()) {
            return AVAILABLE;
        }
        final CompletableFuture<?> anyInputAvailable = new CompletableFuture<>();
        for (int i = 0; i < inputProcessors.length; i++) {
            if (!inputSelectionHandler.isInputFinished(i)
                    && inputSelectionHandler.isInputSelected(i)) {
                assertNoException(
                        inputProcessors[i]
                                .getAvailableFuture()
                                .thenRun(() -> anyInputAvailable.complete(null)));
            }
        }
        return anyInputAvailable;
    }

    @Override
    public InputStatus processInput() throws Exception {
        int readingInputIndex;
        if (isPrepared) {
            readingInputIndex = selectNextReadingInputIndex();
        } else {
            // the preparations here are not placed in the constructor because all work in it
            // must be executed after all operators are opened.
            readingInputIndex = selectFirstReadingInputIndex();
        }
        if (readingInputIndex == InputSelection.NONE_AVAILABLE) {
            return InputStatus.NOTHING_AVAILABLE;
        }

        lastReadInputIndex = readingInputIndex;
        InputStatus inputStatus = inputProcessors[readingInputIndex].processInput();
        inputSelectionHandler.nextSelection();
        return inputSelectionHandler.updateStatus(inputStatus, readingInputIndex);
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
            ChannelStateWriter channelStateWriter, long checkpointId) throws IOException {
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
}
