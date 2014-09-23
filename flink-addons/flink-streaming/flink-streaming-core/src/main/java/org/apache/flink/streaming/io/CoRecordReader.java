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

package org.apache.flink.streaming.io;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.event.task.AbstractTaskEvent;
import org.apache.flink.runtime.io.network.api.AbstractRecordReader;
import org.apache.flink.runtime.io.network.api.MutableRecordReader;
import org.apache.flink.runtime.io.network.gates.InputChannelResult;
import org.apache.flink.runtime.io.network.gates.InputGate;
import org.apache.flink.runtime.io.network.gates.RecordAvailabilityListener;

/**
 * A CoRecordReader wraps {@link MutableRecordReader}s of two different input
 * types to read records effectively.
 */
@SuppressWarnings("rawtypes")
public class CoRecordReader<T1 extends IOReadableWritable, T2 extends IOReadableWritable> extends
		AbstractRecordReader implements RecordAvailabilityListener {

	/**
	 * Sets of input gates for the two input types
	 */
	private Set<InputGate<T1>> inputGates1;
	private Set<InputGate<T2>> inputGates2;

	private final Set<InputGate> remainingInputGates;

	/**
	 * Queue with indices of channels that store at least one available record.
	 */
	private final ArrayDeque<InputGate> availableInputGates = new ArrayDeque<InputGate>();

	/**
	 * The next input gate to read a record from.
	 */
	private InputGate nextInputGateToReadFrom;

	@Override
	public boolean isInputClosed() {
		return this.remainingInputGates.isEmpty();
	}

	@SuppressWarnings("unchecked")
	public CoRecordReader(ArrayList<MutableRecordReader<T1>> inputList1,
			ArrayList<MutableRecordReader<T2>> inputList2) {

		if (inputList1 == null || inputList2 == null) {
			throw new IllegalArgumentException("Provided argument recordReaders is null");
		}

		this.inputGates1 = new HashSet<InputGate<T1>>();
		this.inputGates2 = new HashSet<InputGate<T2>>();
		this.remainingInputGates = new HashSet<InputGate>(
				(int) ((inputGates1.size() + inputGates2.size()) * 1.6f));

		for (MutableRecordReader<T1> reader : inputList1) {
			InputGate<T1> inputGate = (InputGate<T1>) reader.getInputGate();
			inputGate.registerRecordAvailabilityListener(this);
			this.inputGates1.add(inputGate);
			this.remainingInputGates.add(inputGate);
		}

		for (MutableRecordReader<T2> reader : inputList2) {
			InputGate<T2> inputGate = (InputGate<T2>) reader.getInputGate();
			inputGate.registerRecordAvailabilityListener(this);
			this.inputGates2.add(inputGate);
			this.remainingInputGates.add(inputGate);
		}
	}

	@Override
	public void publishEvent(AbstractTaskEvent event) throws IOException, InterruptedException {
		for (InputGate<T1> gate : this.inputGates1) {
			gate.publishEvent(event);
		}
		for (InputGate<T2> gate : this.inputGates2) {
			gate.publishEvent(event);
		}
	}

	@Override
	public void reportRecordAvailability(InputGate inputGate) {
		synchronized (this.availableInputGates) {
			this.availableInputGates.add(inputGate);
			this.availableInputGates.notifyAll();
		}
	}

	@SuppressWarnings("unchecked")
	protected int getNextRecord(T1 target1, T2 target2) throws IOException, InterruptedException {
		int out;
		while (true) {
			// has the current input gate more data?
			if (this.nextInputGateToReadFrom == null) {
				if (this.remainingInputGates.isEmpty()) {
					return 0;
				}

				this.nextInputGateToReadFrom = getNextAvailableInputGate();
			}
			InputChannelResult result;

			if (inputGates1.contains(this.nextInputGateToReadFrom)) {
				result = this.nextInputGateToReadFrom.readRecord(target1);
				out = 1;
			} else {
				result = this.nextInputGateToReadFrom.readRecord(target2);
				out = 2;
			}
			switch (result) {
			case INTERMEDIATE_RECORD_FROM_BUFFER: // record is available and we
													// can stay on the same
													// channel
				return out;

			case LAST_RECORD_FROM_BUFFER: // record is available, but we need to
											// re-check the channels
				this.nextInputGateToReadFrom = null;
				return out;

			case END_OF_SUPERSTEP:
				this.nextInputGateToReadFrom = null;
				if (incrementEndOfSuperstepEventAndCheck()) {
					return 0; // end of the superstep
				} else {
					break; // fall through and wait for next record/event
				}

			case TASK_EVENT: // event for the subscribers is available
				handleEvent(this.nextInputGateToReadFrom.getCurrentEvent());
				this.nextInputGateToReadFrom = null;
				break;

			case END_OF_STREAM: // one gate is empty
				this.remainingInputGates.remove(this.nextInputGateToReadFrom);
				this.nextInputGateToReadFrom = null;
				break;

			case NONE: // gate processed an internal event and could not return
						// a record on this call
				this.nextInputGateToReadFrom = null;
				break;
			}
		}
	}

	private InputGate getNextAvailableInputGate() throws InterruptedException {
		synchronized (this.availableInputGates) {
			while (this.availableInputGates.isEmpty()) {
				this.availableInputGates.wait();
			}
			return this.availableInputGates.pop();
		}
	}
}
