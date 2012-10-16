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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.checkpointing.EphemeralCheckpoint;
import eu.stratosphere.nephele.execution.RuntimeEnvironment;
import eu.stratosphere.nephele.executiongraph.CheckpointState;
import eu.stratosphere.nephele.io.AbstractID;
import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.RuntimeInputGate;
import eu.stratosphere.nephele.io.RuntimeOutputGate;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.compression.CompressionBufferProvider;
import eu.stratosphere.nephele.taskmanager.bufferprovider.AsynchronousEventListener;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferAvailabilityListener;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferProvider;
import eu.stratosphere.nephele.taskmanager.bufferprovider.LocalBufferPool;
import eu.stratosphere.nephele.taskmanager.bytebuffered.InputGateContext;
import eu.stratosphere.nephele.taskmanager.bytebuffered.OutputGateContext;
import eu.stratosphere.nephele.taskmanager.bytebuffered.TaskContext;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelopeDispatcher;
import eu.stratosphere.nephele.types.Record;

public final class RuntimeTaskContext implements BufferProvider, AsynchronousEventListener, TaskContext {

	private static final Log LOG = LogFactory.getLog(RuntimeTaskContext.class);

	private final LocalBufferPool localBufferPool;

	private final RuntimeTask task;

	private final int numberOfOutputChannels;

	private final TransferEnvelopeDispatcher transferEnvelopeDispatcher;

	private final EphemeralCheckpoint ephemeralCheckpoint;

	private final EnvelopeConsumptionLog envelopeConsumptionLog;

	private CompressionBufferProvider compressionBufferProvider = null;

	RuntimeTaskContext(final RuntimeTask task, final CheckpointState initialCheckpointState,
			final TransferEnvelopeDispatcher transferEnvelopeDispatcher) {

		this.localBufferPool = new LocalBufferPool(1, false, this);
		this.task = task;

		final RuntimeEnvironment environment = task.getRuntimeEnvironment();

		// Compute number of output input channels
		int nooc = 0;
		for (int i = 0; i < environment.getNumberOfOutputGates(); ++i) {
			final RuntimeOutputGate<? extends Record> outputGate = environment.getOutputGate(i);
			if (outputGate.isBroadcast()) {
				++nooc;
			} else {
				nooc += outputGate.getNumberOfOutputChannels();
			}
		}
		this.numberOfOutputChannels = nooc;

		if (initialCheckpointState == CheckpointState.NONE) {
			this.ephemeralCheckpoint = null;
		} else {
			this.ephemeralCheckpoint = new EphemeralCheckpoint(task, this.numberOfOutputChannels,
				initialCheckpointState == CheckpointState.UNDECIDED);
			this.task.registerCheckpointDecisionRequester(this.ephemeralCheckpoint);
		}

		this.transferEnvelopeDispatcher = transferEnvelopeDispatcher;
		this.envelopeConsumptionLog = new EnvelopeConsumptionLog(task.getVertexID(), environment);
	}

	TransferEnvelopeDispatcher getTransferEnvelopeDispatcher() {

		return this.transferEnvelopeDispatcher;
	}

	EphemeralCheckpoint getEphemeralCheckpoint() {

		return this.ephemeralCheckpoint;
	}

	/**
	 * Returns (and if necessary previously creates) a compression buffer provider for output gate contexts. This method
	 * must not be called from input gate contexts since input gate contexts are supposed to have their own compression
	 * buffer provider objects.
	 * 
	 * @return the compression buffer provider for the output gate context
	 */
	CompressionBufferProvider getCompressionBufferProvider() {

		if (this.compressionBufferProvider == null) {
			this.compressionBufferProvider = new CompressionBufferProvider(this, false);
		} else {
			this.compressionBufferProvider.increaseReferenceCounter();
		}

		return this.compressionBufferProvider;
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

		// Finish the envelope consumption log
		this.envelopeConsumptionLog.finish();
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

		if (this.envelopeConsumptionLog.followsLog()) {
			this.envelopeConsumptionLog.showOustandingEnvelopeLog();
		}
	}

	/**
	 * Called by an {@link OutputGateContext} to indicate that the task has temporarily run out of memory buffers.
	 */
	void reportExhaustionOfMemoryBuffers() throws IOException, InterruptedException {

		if (this.ephemeralCheckpoint == null) {
			return;
		}

		if (!this.ephemeralCheckpoint.isUndecided()) {
			return;
		}

		// TODO: Remove this and implement decision logic for ephemeral checkpoint
		LOG.error("Checkpoint state of " + this.task.getRuntimeEnvironment().getTaskNameWithIndex() + " is UNDECIDED");
		this.ephemeralCheckpoint.setCheckpointDecisionSynchronously(false);

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

		// Trigger checkpoint decision here
		reportExhaustionOfMemoryBuffers();
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

		RuntimeOutputGate<? extends Record> outputGate = null;
		final RuntimeEnvironment re = this.task.getRuntimeEnvironment();
		for (int i = 0; i < re.getNumberOfOutputGates(); ++i) {
			final RuntimeOutputGate<? extends Record> candidateGate = re.getOutputGate(i);
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

		RuntimeInputGate<? extends Record> inputGate = null;
		final RuntimeEnvironment re = this.task.getRuntimeEnvironment();
		for (int i = 0; i < re.getNumberOfInputGates(); ++i) {
			final RuntimeInputGate<? extends Record> candidateGate = re.getInputGate(i);
			if (candidateGate.getGateID().equals(gateID)) {
				inputGate = candidateGate;
				break;
			}
		}

		if (inputGate == null) {
			throw new IllegalStateException("Cannot find input gate with ID " + gateID);
		}

		return new RuntimeInputGateContext(re.getTaskNameWithIndex(), this.transferEnvelopeDispatcher, inputGate,
			this.envelopeConsumptionLog);
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
