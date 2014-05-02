/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.io;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Set;

import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.nephele.event.task.AbstractTaskEvent;

public abstract class AbstractUnionRecordReader<T extends IOReadableWritable> extends AbstractRecordReader implements RecordAvailabilityListener<T> {

	/**
	 * The set of all input gates.
	 */
	private final InputGate<T>[] allInputGates;
	
	/**
	 * The set of unclosed input gates.
	 */
	private final Set<InputGate<T>> remainingInputGates;

	/**
	 * Queue with indices of channels that store at least one available record.
	 */
	private final ArrayDeque<InputGate<T>> availableInputGates = new ArrayDeque<InputGate<T>>();
	
	/**
	 * The next input gate to read a record from.
	 */
	private InputGate<T> nextInputGateToReadFrom;

	
	@Override
	public boolean isInputClosed() {
		return this.remainingInputGates.isEmpty();
	}
	
	/**
	 * Constructs a new mutable union record reader.
	 * 
	 * @param recordReaders
	 *        the individual mutable record readers whose input is used to construct the union
	 */
	@SuppressWarnings("unchecked")
	protected AbstractUnionRecordReader(MutableRecordReader<T>[] recordReaders) {

		if (recordReaders == null) {
			throw new IllegalArgumentException("Provided argument recordReaders is null");
		}

		if (recordReaders.length < 2) {
			throw new IllegalArgumentException(
				"The mutable union record reader must at least be initialized with two individual mutable record readers");
		}
		
		this.allInputGates = new InputGate[recordReaders.length];
		this.remainingInputGates = new HashSet<InputGate<T>>((int) (recordReaders.length * 1.6f));
		
		for (int i = 0; i < recordReaders.length; i++) {
			InputGate<T> inputGate = recordReaders[i].getInputGate();
			inputGate.registerRecordAvailabilityListener(this);
			this.allInputGates[i] = inputGate;
			this.remainingInputGates.add(inputGate);
		}
	}
	
	
	@Override
	public void publishEvent(AbstractTaskEvent event) throws IOException, InterruptedException {
		for (InputGate<T> gate : this.allInputGates) {
			gate.publishEvent(event);
		}
	}
	
	@Override
	public void reportRecordAvailability(InputGate<T> inputGate) {
		synchronized (this.availableInputGates) {
			this.availableInputGates.add(inputGate);
			this.availableInputGates.notifyAll();
		}
	}
	
	protected boolean getNextRecord(T target) throws IOException, InterruptedException {

		while (true) {
			// has the current input gate more data?
			if (this.nextInputGateToReadFrom == null) {
				if (this.remainingInputGates.isEmpty()) {
					return false;
				}
				
				this.nextInputGateToReadFrom = getNextAvailableInputGate();
			}

			InputChannelResult result = this.nextInputGateToReadFrom.readRecord(target);
			switch (result) {
				case INTERMEDIATE_RECORD_FROM_BUFFER: // record is available and we can stay on the same channel
					return true;
					
				case LAST_RECORD_FROM_BUFFER: // record is available, but we need to re-check the channels
					this.nextInputGateToReadFrom = null;
					return true;
					
				case END_OF_SUPERSTEP:
					this.nextInputGateToReadFrom = null;
					if (incrementEndOfSuperstepEventAndCheck()) {
						return false; // end of the superstep
					}
					else {
						break; // fall through and wait for next record/event
					}
					
				case TASK_EVENT:	// event for the subscribers is available
					handleEvent(this.nextInputGateToReadFrom.getCurrentEvent());
					this.nextInputGateToReadFrom = null;
					break;
					
				case END_OF_STREAM: // one gate is empty
					this.remainingInputGates.remove(this.nextInputGateToReadFrom);
					this.nextInputGateToReadFrom = null;
					break;
					
				case NONE: // gate processed an internal event and could not return a record on this call
					this.nextInputGateToReadFrom = null;
					break;
			}
		}
	}
	
	private InputGate<T> getNextAvailableInputGate() throws InterruptedException {
		synchronized (this.availableInputGates) {
			while (this.availableInputGates.isEmpty()) {
				this.availableInputGates.wait();
			}
			return this.availableInputGates.pop();
		}
	}
}
