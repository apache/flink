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

package eu.stratosphere.nephele.taskmanager.runtime;

import java.io.IOException;

import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.nephele.execution.RuntimeEnvironment;
import eu.stratosphere.nephele.io.AbstractID;
import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferAvailabilityListener;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferProvider;
import eu.stratosphere.nephele.taskmanager.bufferprovider.LocalBufferPool;
import eu.stratosphere.nephele.taskmanager.bytebuffered.InputGateContext;
import eu.stratosphere.nephele.taskmanager.bytebuffered.OutputGateContext;
import eu.stratosphere.nephele.taskmanager.bytebuffered.TaskContext;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelopeDispatcher;

public final class RuntimeTaskContext implements BufferProvider, TaskContext {

	private final LocalBufferPool localBufferPool;

	private final RuntimeTask task;

	private final int numberOfOutputChannels;

	private final TransferEnvelopeDispatcher transferEnvelopeDispatcher;

	RuntimeTaskContext(final RuntimeTask task, final TransferEnvelopeDispatcher transferEnvelopeDispatcher) {

		this.localBufferPool = new LocalBufferPool(1, false);
		this.task = task;

		final RuntimeEnvironment environment = task.getRuntimeEnvironment();

		// Compute number of output input channels
		int nooc = 0;
		for (int i = 0; i < environment.getNumberOfOutputGates(); ++i) {
			final OutputGate<? extends IOReadableWritable> outputGate = environment.getOutputGate(i);
			if (outputGate.isBroadcast()) {
				++nooc;
			} else {
				nooc += outputGate.getNumberOfOutputChannels();
			}
		}
		this.numberOfOutputChannels = nooc;

		this.transferEnvelopeDispatcher = transferEnvelopeDispatcher;
	}

	TransferEnvelopeDispatcher getTransferEnvelopeDispatcher() {

		return this.transferEnvelopeDispatcher;
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public Buffer requestEmptyBuffer(final int minimumSizeOfBuffer) throws IOException {

		return this.localBufferPool.requestEmptyBuffer(minimumSizeOfBuffer);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Buffer requestEmptyBufferBlocking(int minimumSizeOfBuffer) throws IOException,
			InterruptedException {

		return this.localBufferPool.requestEmptyBufferBlocking(minimumSizeOfBuffer);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getMaximumBufferSize() {

		return this.localBufferPool.getMaximumBufferSize();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void clearLocalBufferPool() {

		// Clear the buffer cache
		this.localBufferPool.destroy();

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isShared() {

		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void logBufferUtilization() {

		final int ava = this.localBufferPool.getNumberOfAvailableBuffers();
		final int req = this.localBufferPool.getRequestedNumberOfBuffers();
		final int des = this.localBufferPool.getDesignatedNumberOfBuffers();

		final RuntimeEnvironment environment = this.task.getRuntimeEnvironment();

		System.out.println("\t\t" + environment.getTaskNameWithIndex() + ": " + ava + " available, " + req
			+ " requested, " + des + " designated");
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public void reportAsynchronousEvent() {

		this.localBufferPool.reportAsynchronousEvent();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getNumberOfChannels() {

		return this.numberOfOutputChannels;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setDesignatedNumberOfBuffers(int numberOfBuffers) {

		this.localBufferPool.setDesignatedNumberOfBuffers(numberOfBuffers);
	}

	AbstractID getFileOwnerID() {

		return this.task.getVertexID();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public OutputGateContext createOutputGateContext(final GateID gateID) {

		if (gateID == null) {
			throw new IllegalArgumentException("Argument gateID must not be null");
		}

		OutputGate<? extends IOReadableWritable> outputGate = null;
		final RuntimeEnvironment re = this.task.getRuntimeEnvironment();
		for (int i = 0; i < re.getNumberOfOutputGates(); ++i) {
			final OutputGate<? extends IOReadableWritable> candidateGate = re.getOutputGate(i);
			if (candidateGate.getGateID().equals(gateID)) {
				outputGate = candidateGate;
				break;
			}
		}

		if (outputGate == null) {
			throw new IllegalStateException("Cannot find output gate with ID " + gateID);
		}

		return new RuntimeOutputGateContext(this, outputGate);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public InputGateContext createInputGateContext(final GateID gateID) {

		if (gateID == null) {
			throw new IllegalArgumentException("Argument gateID must not be null");
		}

		InputGate<? extends IOReadableWritable> inputGate = null;
		final RuntimeEnvironment re = this.task.getRuntimeEnvironment();
		for (int i = 0; i < re.getNumberOfInputGates(); ++i) {
			final InputGate<? extends IOReadableWritable> candidateGate = re.getInputGate(i);
			if (candidateGate.getGateID().equals(gateID)) {
				inputGate = candidateGate;
				break;
			}
		}

		if (inputGate == null) {
			throw new IllegalStateException("Cannot find input gate with ID " + gateID);
		}

		return new RuntimeInputGateContext(re.getTaskNameWithIndex(), this.transferEnvelopeDispatcher, inputGate);
	}

	public LocalBufferPool getLocalBufferPool() {

		return this.localBufferPool;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean registerBufferAvailabilityListener(final BufferAvailabilityListener bufferAvailabilityListener) {

		return this.localBufferPool.registerBufferAvailabilityListener(bufferAvailabilityListener);
	}
}
