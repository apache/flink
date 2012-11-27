/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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
import java.util.NoSuchElementException;
import java.util.Set;

import eu.stratosphere.nephele.types.Record;

public final class UnionRecordReader<T extends Record> implements Reader<T>, RecordAvailabilityListener<T> {

	private final Set<InputGate<T>> inputGates;

	private InputGate<T> nextInputGateToReadFrom = null;

	private IOException ioException = null;

	private InterruptedException interruptedExecption = null;

	/**
	 * Queue with indices of channels that store at least one available record.
	 */
	private final ArrayDeque<InputGate<T>> availableInputGates = new ArrayDeque<InputGate<T>>();

	private T nextRecord = null;

	public UnionRecordReader(final RecordReader<T>[] recordReaders) {

		if (recordReaders == null) {
			throw new IllegalArgumentException("Provided argument recordReaders is null");
		}

		if (recordReaders.length < 2) {
			throw new IllegalArgumentException(
				"The union record reader must at least be initialized with two individual record readers");
		}

		this.inputGates = new HashSet<InputGate<T>>(recordReaders.length);
		for (final RecordReader<T> rr : recordReaders) {
			final InputGate<T> inputGate = rr.getInputGate();
			inputGate.registerRecordAvailabilityListener(this);
			this.inputGates.add(inputGate);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean hasNext() {

		if (this.nextRecord != null) {
			return true;
		}

		try {
			this.nextRecord = readNextRecord();
		} catch (IOException ioe) {
			this.ioException = ioe;
			return true;
		} catch (InterruptedException ie) {
			this.interruptedExecption = ie;
			return true;
		}

		if (this.nextRecord != null) {
			return true;
		}

		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T next() throws IOException, InterruptedException {

		if (this.nextRecord == null && this.inputGates.isEmpty()) {
			throw new NoSuchElementException();
		}

		if (this.ioException != null) {
			throw this.ioException;
		}

		if (this.interruptedExecption != null) {
			throw this.interruptedExecption;
		}

		if (this.nextRecord == null) {
			this.nextRecord = readNextRecord();
		}

		T retVal = this.nextRecord;
		this.nextRecord = null;

		return retVal;
	}

	/**
	 * Reads the next record from one of the underlying input gates.
	 * 
	 * @return the next record from the underlying input gates or <code>null</code> if all underlying input gates are
	 *         closed.
	 * @throws IOException
	 *         thrown if one of the underlying input gates experienced an IOException
	 * @throws InterruptedException
	 *         thrown if one of the underlying input gates experienced an InterruptedException
	 */
	private T readNextRecord() throws IOException, InterruptedException {

		while (true) {

			if (this.inputGates.isEmpty()) {
				return null;
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

				final T record = this.nextInputGateToReadFrom.readRecord(null);
				if (record == null) { // Gate is closed
					this.inputGates.remove(this.nextInputGateToReadFrom);
					this.nextInputGateToReadFrom = null;
				} else {
					return record;
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
}
