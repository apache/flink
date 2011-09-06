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

package eu.stratosphere.nephele.taskmanager.bytebuffered;

import java.io.IOException;

import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.io.AbstractID;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.taskmanager.Task;
import eu.stratosphere.nephele.taskmanager.bufferprovider.AsynchronousEventListener;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferProvider;
import eu.stratosphere.nephele.taskmanager.bufferprovider.LocalBufferPool;
import eu.stratosphere.nephele.taskmanager.bufferprovider.LocalBufferPoolOwner;
import eu.stratosphere.nephele.taskmanager.checkpointing.EphemeralCheckpoint;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelopeDispatcher;
import eu.stratosphere.nephele.types.Record;

final class TaskContext implements BufferProvider, LocalBufferPoolOwner, AsynchronousEventListener {

	private final LocalBufferPool localBufferPool;

	private final Task task;

	private final AsynchronousEventListener[] subEventListener;

	private final int numberOfOutputChannels;

	final TransferEnvelopeDispatcher transferEnvelopeDispatcher;

	private final EphemeralCheckpoint ephemeralCheckpoint;

	private final boolean forwardTransferEnvelopes;

	/**
	 * Stores whether the initial exhaustion of memory buffers has already been reported
	 */
	private boolean initialExhaustionOfMemoryBuffersReported = false;

	TaskContext(final Task task, final TransferEnvelopeDispatcher transferEnvelopeDispatcher) {

		this.localBufferPool = new LocalBufferPool(1, false, this);
		this.task = task;

		final Environment environment = task.getEnvironment();

		// Compute number of output input channels
		int nooc = 0;
		boolean ephemeral = true;
		for (int i = 0; i < environment.getNumberOfOutputGates(); ++i) {
			final OutputGate<? extends Record> outputGate = environment.getOutputGate(i);
			nooc += outputGate.getNumberOfOutputChannels();
			if (outputGate.getChannelType() == ChannelType.FILE) {
				ephemeral = false;
			}
		}
		this.numberOfOutputChannels = nooc;
		this.forwardTransferEnvelopes = ephemeral;

		this.ephemeralCheckpoint = new EphemeralCheckpoint(task.getVertexID(), this.numberOfOutputChannels, ephemeral);

		this.transferEnvelopeDispatcher = transferEnvelopeDispatcher;

		// Each output gate context will register as a sub event listener
		this.subEventListener = new AsynchronousEventListener[environment.getNumberOfOutputGates()];
	}

	void registerAsynchronousEventListener(final int index, final AsynchronousEventListener eventListener) {

		if (index >= this.subEventListener.length || index < 0) {
			throw new IllegalArgumentException("Argument index has invalid value " + index);
		}

		if (eventListener == null) {
			throw new IllegalArgumentException("Argument eventListener must not be null");
		}

		if (this.subEventListener[index] != null) {
			throw new IllegalStateException("There is already an event listener with index " + index + " registered");
		}

		this.subEventListener[index] = eventListener;
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
		this.localBufferPool.clear();
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

		final Environment environment = this.task.getEnvironment();

		System.out.println("\t\t" + environment.getTaskName() + ": " + ava + " available, " + req + " requested, "
			+ des + " designated");
	}

	/**
	 * Called by an {@link OutputGateContext} to indicate that the task has temporarily run out of memory buffers.
	 */
	void reportExhaustionOfMemoryBuffers() {

		if (!this.initialExhaustionOfMemoryBuffersReported) {

			this.task.initialExecutionResourcesExhausted();

			// We are out of byte buffers
			if (!this.ephemeralCheckpoint.isDecided()) {
				System.out.println("Destroying checkpoint");
				this.ephemeralCheckpoint.destroy();
			}

			this.initialExhaustionOfMemoryBuffersReported = true;
		}
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
	public void asynchronousEventOccurred() throws IOException, InterruptedException {

		for (int i = 0; i < this.subEventListener.length; ++i) {

			if (this.subEventListener[i] == null) {
				throw new IllegalStateException("Event listener at index " + i + " is null");
			}

			this.subEventListener[i].asynchronousEventOccurred();
		}
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

	/**
	 * Called by the attached output gate context to forward a {@link TransferEnvelope} object
	 * to its final destination. Within this method the provided transfer envelope is possibly also
	 * forwarded to the assigned ephemeral checkpoint.
	 * 
	 * @param outgoingTransferEnvelope
	 *        the transfer envelope to be forwarded
	 * @throws IOException
	 *         thrown if an I/O error occurs while processing the envelope
	 * @throws InterruptedException
	 *         thrown if the thread is interrupted while waiting for the envelope to be processed
	 */
	void processEnvelope(final TransferEnvelope outgoingTransferEnvelope) throws IOException, InterruptedException {

		if (!this.ephemeralCheckpoint.isDiscarded()) {
			final TransferEnvelope dup = outgoingTransferEnvelope.duplicate();
			this.ephemeralCheckpoint.addTransferEnvelope(dup);
		}

		if (this.forwardTransferEnvelopes) {
			// Immediately forward the envelope
			this.transferEnvelopeDispatcher.processEnvelopeFromOutputChannel(outgoingTransferEnvelope);
		} else {
			// Simply discard the envelope
			final Buffer buffer = outgoingTransferEnvelope.getBuffer();
			if (buffer != null) {
				buffer.recycleBuffer();
			}
		}
	}

	AbstractID getFileOwnerID() {

		return this.task.getVertexID();
	}
}
