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

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This handler is mainly used for selecting the next available input index
 * in {@link StreamTwoInputSelectableProcessor}.
 */
@Internal
public class TwoInputSelectionHandler {

	private final InputSelectable inputSelector;

	private InputSelection inputSelection;

	private int availableInputsMask;

	public TwoInputSelectionHandler(InputSelectable inputSelectable) {
		this.inputSelector = checkNotNull(inputSelectable);
		this.availableInputsMask = (int) new InputSelection.Builder().select(1).select(2).build().getInputMask();
	}

	void nextSelection() {
		inputSelection = inputSelector.nextSelection();
	}

	int selectNextInputIndex(int lastReadInputIndex) {
		return inputSelection.fairSelectNextIndexOutOf2(availableInputsMask, lastReadInputIndex);
	}

	boolean shouldSetAvailableForAnotherInput() {
		return availableInputsMask < 3 && inputSelection.isALLMaskOf2();
	}

	void setAvailableInput(int inputIndex) {
		availableInputsMask |= 1 << inputIndex;
	}

	void setUnavailableInput(int inputIndex) {
		availableInputsMask &= ~(1 << inputIndex);
	}

	boolean areAllInputsSelected() {
		return inputSelection.isALLMaskOf2();
	}

	boolean isFirstInputSelected() {
		return inputSelection.isInputSelected(1);
	}

	boolean isSecondInputSelected() {
		return inputSelection.isInputSelected(2);
	}
}
