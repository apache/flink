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
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.streaming.runtime.tasks.TwoInputStreamTask;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * Input reader for {@link TwoInputStreamTask}.
 *
 * @param <IN1> The type of the records that arrive on the first input
 * @param <IN2> The type of the records that arrive on the second input
 */
@Internal
public final class StreamTwoInputProcessor<IN1, IN2> implements StreamInputProcessor {

    private final TwoInputSelectionHandler inputSelectionHandler;

    private final StreamOneInputProcessor<IN1> processor1;
    private final StreamOneInputProcessor<IN2> processor2;

    /** Input status to keep track for determining whether the input is finished or not. */
    private InputStatus firstInputStatus = InputStatus.MORE_AVAILABLE;

    private InputStatus secondInputStatus = InputStatus.MORE_AVAILABLE;

    /** Always try to read from the first input. */
    private int lastReadInputIndex = 1;

    private boolean isPrepared;

    public StreamTwoInputProcessor(
            TwoInputSelectionHandler inputSelectionHandler,
            StreamOneInputProcessor<IN1> processor1,
            StreamOneInputProcessor<IN2> processor2) {
        this.inputSelectionHandler = inputSelectionHandler;
        this.processor1 = processor1;
        this.processor2 = processor2;
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        if (inputSelectionHandler.areAllInputsSelected()) {
            return isAnyInputAvailable();
        } else {
            StreamOneInputProcessor<?> input =
                    (inputSelectionHandler.isFirstInputSelected()) ? processor1 : processor2;
            return input.getAvailableFuture();
        }
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
        // In case of double notification (especially with priority notification), there may not be
        // an input after all.
        if (readingInputIndex == InputSelection.NONE_AVAILABLE) {
            return InputStatus.NOTHING_AVAILABLE;
        }

        lastReadInputIndex = readingInputIndex;

        if (readingInputIndex == 0) {
            firstInputStatus = processor1.processInput();
        } else {
            secondInputStatus = processor2.processInput();
        }
        inputSelectionHandler.nextSelection();

        return getInputStatus();
    }

    @Override
    public CompletableFuture<Void> prepareSnapshot(
            ChannelStateWriter channelStateWriter, long checkpointId) throws IOException {
        return CompletableFuture.allOf(
                processor1.prepareSnapshot(channelStateWriter, checkpointId),
                processor2.prepareSnapshot(channelStateWriter, checkpointId));
    }

    private int selectFirstReadingInputIndex() throws IOException {
        // Note: the first call to nextSelection () on the operator must be made after this operator
        // is opened to ensure that any changes about the input selection in its open()
        // method take effect.
        inputSelectionHandler.nextSelection();

        isPrepared = true;

        return selectNextReadingInputIndex();
    }

    private InputStatus getInputStatus() {
        if (firstInputStatus == InputStatus.END_OF_INPUT
                && secondInputStatus == InputStatus.END_OF_INPUT) {
            return InputStatus.END_OF_INPUT;
        }

        if (inputSelectionHandler.areAllInputsSelected()) {
            if (firstInputStatus == InputStatus.MORE_AVAILABLE
                    || secondInputStatus == InputStatus.MORE_AVAILABLE) {
                return InputStatus.MORE_AVAILABLE;
            } else {
                return InputStatus.NOTHING_AVAILABLE;
            }
        }

        InputStatus selectedStatus =
                inputSelectionHandler.isFirstInputSelected() ? firstInputStatus : secondInputStatus;
        InputStatus otherStatus =
                inputSelectionHandler.isFirstInputSelected() ? secondInputStatus : firstInputStatus;
        return selectedStatus == InputStatus.END_OF_INPUT ? otherStatus : selectedStatus;
    }

    @Override
    public void close() throws IOException {
        IOException ex = null;
        try {
            processor1.close();
        } catch (IOException e) {
            ex = ExceptionUtils.firstOrSuppressed(e, ex);
        }

        try {
            processor2.close();
        } catch (IOException e) {
            ex = ExceptionUtils.firstOrSuppressed(e, ex);
        }

        if (ex != null) {
            throw ex;
        }
    }

    private int selectNextReadingInputIndex() throws IOException {
        updateAvailability();
        checkInputSelectionAgainstIsFinished();

        int readingInputIndex = inputSelectionHandler.selectNextInputIndex(lastReadInputIndex);
        if (readingInputIndex == InputSelection.NONE_AVAILABLE) {
            return InputSelection.NONE_AVAILABLE;
        }

        // to avoid starvation, if the input selection is ALL and availableInputsMask is not ALL,
        // always try to check and set the availability of another input
        if (inputSelectionHandler.shouldSetAvailableForAnotherInput()) {
            checkAndSetAvailable(1 - readingInputIndex);
        }

        return readingInputIndex;
    }

    private void checkInputSelectionAgainstIsFinished() throws IOException {
        if (inputSelectionHandler.areAllInputsSelected()) {
            return;
        }
        if (inputSelectionHandler.isFirstInputSelected()
                && firstInputStatus == InputStatus.END_OF_INPUT) {
            throw new IOException(
                    "Can not make a progress: only first input is selected but it is already finished");
        }
        if (inputSelectionHandler.isSecondInputSelected()
                && secondInputStatus == InputStatus.END_OF_INPUT) {
            throw new IOException(
                    "Can not make a progress: only second input is selected but it is already finished");
        }
    }

    private void updateAvailability() {
        updateAvailability(firstInputStatus, processor1, 0);
        updateAvailability(secondInputStatus, processor2, 1);
    }

    private void updateAvailability(
            InputStatus status, StreamOneInputProcessor<?> input, int inputIdx) {
        if (status == InputStatus.MORE_AVAILABLE
                || (status != InputStatus.END_OF_INPUT && input.isApproximatelyAvailable())) {
            inputSelectionHandler.setAvailableInput(inputIdx);
        } else {
            inputSelectionHandler.setUnavailableInput(inputIdx);
        }
    }

    private void checkAndSetAvailable(int inputIndex) {
        InputStatus status = (inputIndex == 0 ? firstInputStatus : secondInputStatus);
        if (status == InputStatus.END_OF_INPUT) {
            return;
        }

        // TODO: isAvailable() can be a costly operation (checking volatile). If one of
        // the input is constantly available and another is not, we will be checking this volatile
        // once per every record. This might be optimized to only check once per processed
        // NetworkBuffer
        if (getInput(inputIndex).isAvailable()) {
            inputSelectionHandler.setAvailableInput(inputIndex);
        }
    }

    private CompletableFuture<?> isAnyInputAvailable() {
        if (firstInputStatus == InputStatus.END_OF_INPUT) {
            return processor2.getAvailableFuture();
        }

        if (secondInputStatus == InputStatus.END_OF_INPUT) {
            return processor1.getAvailableFuture();
        }

        return AvailabilityProvider.or(
                processor1.getAvailableFuture(), processor2.getAvailableFuture());
    }

    private StreamOneInputProcessor<?> getInput(int inputIndex) {
        return inputIndex == 0 ? processor1 : processor2;
    }
}
