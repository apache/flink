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

package org.apache.flink.table.runtime.operators.multipleinput.input;

import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.table.runtime.operators.multipleinput.MultipleInputStreamOperatorBase;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * This handler is mainly used for selecting the next available input index
 * according to read priority in {@link MultipleInputStreamOperatorBase}.
 *
 * <p>Input read order: the input with high priority (the value of read order is lower)
 * will be read first, the inputs with same priorities will be read fairly.
 */
public class InputSelectionHandler {
	private final List<InputSpec> inputSpecs;
	private final int numberOfInput;
	/** All inputs ids sorted by priority. */
	private final List<List<Integer>> sortedAvailableInputs;
	private InputSelection inputSelection;

	public InputSelectionHandler(List<InputSpec> inputSpecs) {
		this.inputSpecs = inputSpecs;
		this.numberOfInput = inputSpecs.size();
		this.sortedAvailableInputs = buildSortedAvailableInputs();
		// read the highest priority inputs first
		this.inputSelection = buildInputSelection(sortedAvailableInputs.get(0));
	}

	public InputSelection getInputSelection() {
		return inputSelection;
	}

	public void endInput(int inputId) {
		List<Integer> inputIds = sortedAvailableInputs.get(0);
		checkState(inputIds.remove(Integer.valueOf(inputId)), "This should not happen.");
		if (inputIds.isEmpty()) {
			// remove the finished input
			sortedAvailableInputs.remove(0);

			if (sortedAvailableInputs.isEmpty()) {
				// all inputs are finished
				inputIds = new ArrayList<>();
			} else {
				// read next one
				inputIds = sortedAvailableInputs.get(0);
			}
			inputSelection = buildInputSelection(inputIds);
		}
	}

	private List<List<Integer>> buildSortedAvailableInputs() {
		final SortedMap<Integer, List<Integer>> orderedAvailableInputIds = new TreeMap<>();
		for (InputSpec inputSpec : inputSpecs) {
			List<Integer> inputIds = orderedAvailableInputIds
					.computeIfAbsent(inputSpec.getReadOrder(), k -> new LinkedList<>());
			inputIds.add(inputSpec.getMultipleInputId());
		}
		return new LinkedList<>(orderedAvailableInputIds.values());
	}

	private InputSelection buildInputSelection(List<Integer> inputIds) {
		if (inputIds.isEmpty()) {
			// even all inputs are finished, an InputSelection instance should be returned.
			// see StreamMultipleInputProcessor#processInput
			return InputSelection.ALL;
		}
		InputSelection.Builder builder = new InputSelection.Builder();
		inputIds.forEach(builder::select);
		return builder.build(numberOfInput);
	}
}
