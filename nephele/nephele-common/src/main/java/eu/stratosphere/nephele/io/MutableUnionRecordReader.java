/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.io;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Set;

import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.event.task.EventListener;
import eu.stratosphere.nephele.types.Record;

public final class MutableUnionRecordReader<T extends Record> implements RecordAvailabilityListener<T>, MutableReader<T> {

	/**
	 * The set of unclosed input gates.
	 */
	private final Set<InputGate<T>> inputGates;

	/**
	 * The next input gate to read a record from.
	 */
	private InputGate<T> nextInputGateToReadFrom = null;

	/**
	 * Queue with indices of channels that store at least one available record.
	 */
	private final ArrayDeque<InputGate<T>> availableInputGates = new ArrayDeque<InputGate<T>>();

	/**
	 * Constructs a new mutable union record reader.
	 * 
	 * @param recordReaders
	 *        the individual mutable record readers whose input is used to construct the union
	 */
	public MutableUnionRecordReader(final MutableRecordReader<T>[] recordReaders) {

		if (recordReaders == null) {
			throw new IllegalArgumentException("Provided argument recordReaders is null");
		}

		if (recordReaders.length < 2) {
			throw new IllegalArgumentException(
				"The mutable union record reader must at least be initialized with two individual mutable record readers");
		}

		this.inputGates = new HashSet<InputGate<T>>(recordReaders.length);
		for (final MutableRecordReader<T> rr : recordReaders) {
			final InputGate<T> inputGate = rr.getInputGate();
			inputGate.registerRecordAvailabilityListener(this);
			this.inputGates.add(inputGate);
		}
	}

	@Override
	public boolean next(final T target) throws IOException, InterruptedException {

		while (true) {

			if (this.inputGates.isEmpty()) {
				return false;
			}

			if (this.nextInputGateToReadFrom == null) {

				synchronized (this.availableInputGates) {

					while (this.availableInputGates.isEmpty()) {
						this.availableInputGates.wait();
					}

					this.nextInputGateToReadFrom = this.availableInputGates.pop();
				}
			}

			if (this.nextInputGateToReadFrom.hasRecordAvailable()) {

				final T record = this.nextInputGateToReadFrom.readRecord(target);
				if (record == null) { // Gate is closed
					this.inputGates.remove(this.nextInputGateToReadFrom);
					this.nextInputGateToReadFrom = null;
				} else {
					return true;
				}
			} else {
				this.nextInputGateToReadFrom = null;
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void reportRecordAvailability(final InputGate<T> inputGate) {

		synchronized (this.availableInputGates) {
			this.availableInputGates.add(inputGate);
			this.availableInputGates.notify();
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.MutableReader#subscribeToEvent(eu.stratosphere.nephele.event.task.EventListener, java.lang.Class)
	 */
	@Override
	public void subscribeToEvent(EventListener eventListener, Class<? extends AbstractTaskEvent> eventType) {
		for (InputGate<T> gate : this.inputGates) {
			gate.subscribeToEvent(eventListener, eventType);
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.MutableReader#unsubscribeFromEvent(eu.stratosphere.nephele.event.task.EventListener, java.lang.Class)
	 */
	@Override
	public void unsubscribeFromEvent(EventListener eventListener, Class<? extends AbstractTaskEvent> eventType) {
		for (InputGate<T> gate : this.inputGates) {
			gate.unsubscribeFromEvent(eventListener, eventType);
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.MutableReader#publishEvent(eu.stratosphere.nephele.event.task.AbstractTaskEvent)
	 */
	@Override
	public void publishEvent(AbstractTaskEvent event) throws IOException, InterruptedException {
		for (InputGate<T> gate : this.inputGates) {
			gate.publishEvent(event);
		}
	}
}
