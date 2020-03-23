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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.InputSelection;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * This handler is mainly used for selecting the next available input index
 * in {@link StreamMultipleInputProcessor}.
 */
@Internal
public class MultipleInputSelectionHandler {
	public static final int MAX_SUPPORTED_INPUT_COUNT = Long.SIZE;

	@Nullable
	private final InputSelectable inputSelector;

	private InputSelection inputSelection = InputSelection.ALL;

	private final long allSelectedMask;

	private long availableInputsMask;

	private long notFinishedInputsMask;

	public MultipleInputSelectionHandler(@Nullable InputSelectable inputSelectable, int inputCount) {
		checkSupportedInputCount(inputCount);
		this.inputSelector = inputSelectable;
		this.allSelectedMask = (1 << inputCount) - 1;
		this.availableInputsMask = allSelectedMask;
		this.notFinishedInputsMask = allSelectedMask;
	}

	public static void checkSupportedInputCount(int inputCount) {
		checkArgument(
			inputCount <= MAX_SUPPORTED_INPUT_COUNT,
			"Only up to %d inputs are supported at once, while encountered %d",
			MAX_SUPPORTED_INPUT_COUNT,
			inputCount);
	}

	public InputStatus updateStatus(InputStatus inputStatus, int inputIndex) throws IOException {
		switch (inputStatus) {
			case MORE_AVAILABLE:
				checkState(checkBitMask(availableInputsMask, inputIndex));
				return InputStatus.MORE_AVAILABLE;
			case NOTHING_AVAILABLE:
				availableInputsMask = unsetBitMask(availableInputsMask, inputIndex);
				break;
			case END_OF_INPUT:
				notFinishedInputsMask = unsetBitMask(notFinishedInputsMask, inputIndex);
				break;
			default:
				throw new UnsupportedOperationException("Unsupported inputStatus = " + inputStatus);
		}
		return calculateOverallStatus();
	}

	public InputStatus calculateOverallStatus() throws IOException {
		if (areAllInputsFinished()) {
			return InputStatus.END_OF_INPUT;
		}

		if (isAnyInputAvailable()) {
			return InputStatus.MORE_AVAILABLE;
		}
		else {
			long selectedNotFinishedInputMask = inputSelection.getInputMask() & notFinishedInputsMask;
			if (selectedNotFinishedInputMask == 0) {
				throw new IOException("Can not make a progress: all selected inputs are already finished");
			}
			return InputStatus.NOTHING_AVAILABLE;
		}
	}

	void nextSelection() {
		if (inputSelector == null) {
			inputSelection = InputSelection.ALL;
		} else {
			inputSelection = inputSelector.nextSelection();
		}
	}

	int selectNextInputIndex(int lastReadInputIndex) {
		return inputSelection.fairSelectNextIndex(
			availableInputsMask & notFinishedInputsMask,
			lastReadInputIndex);
	}

	boolean shouldSetAvailableForAnotherInput() {
		return availableInputsMask != allSelectedMask && inputSelection.areAllInputsSelected();
	}

	void setAvailableInput(int inputIndex) {
		availableInputsMask = setBitMask(availableInputsMask, inputIndex);
	}

	void setUnavailableInput(int inputIndex) {
		availableInputsMask = unsetBitMask(availableInputsMask, inputIndex);
	}

	boolean isAnyInputAvailable() {
		return (inputSelection.getInputMask() & availableInputsMask & notFinishedInputsMask) != 0;
	}

	boolean areAllInputsSelected() {
		return inputSelection.areAllInputsSelected();
	}

	boolean isInputSelected(int inputIndex) {
		return inputSelection.isInputSelected(inputIndex + 1);
	}

	public boolean isInputFinished(int inputIndex) {
		return !checkBitMask(notFinishedInputsMask, inputIndex);
	}

	public boolean areAllInputsFinished() {
		return notFinishedInputsMask == 0;
	}

	long setBitMask(long mask, int inputIndex) {
		return mask | 1L << inputIndex;
	}

	long unsetBitMask(long mask, int inputIndex) {
		return mask & ~(1L << inputIndex);
	}

	boolean checkBitMask(long mask, int inputIndex) {
		return (mask & (1L << inputIndex)) != 0;
	}
}
