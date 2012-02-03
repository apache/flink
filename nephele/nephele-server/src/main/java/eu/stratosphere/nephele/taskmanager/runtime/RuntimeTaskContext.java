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
import java.util.Map;

import eu.stratosphere.nephele.checkpointing.EphemeralCheckpoint;
import eu.stratosphere.nephele.execution.RuntimeEnvironment;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.AbstractID;
import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.taskmanager.bufferprovider.AsynchronousEventListener;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferProvider;
import eu.stratosphere.nephele.taskmanager.bufferprovider.LocalBufferPool;
import eu.stratosphere.nephele.taskmanager.bufferprovider.LocalBufferPoolOwner;
import eu.stratosphere.nephele.taskmanager.bytebuffered.InputGateContext;
import eu.stratosphere.nephele.taskmanager.bytebuffered.OutputGateContext;
import eu.stratosphere.nephele.taskmanager.bytebuffered.TaskContext;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelopeDispatcher;
import eu.stratosphere.nephele.types.Record;

public final class RuntimeTaskContext implements BufferProvider, AsynchronousEventListener, LocalBufferPoolOwner,
		TaskContext {

	private final LocalBufferPool localBufferPool;

	private final RuntimeTask task;

	private final int numberOfOutputChannels;

	private final TransferEnvelopeDispatcher transferEnvelopeDispatcher;

	private final RuntimeDispatcher runtimeDispatcher;

	private final EphemeralCheckpoint ephemeralCheckpoint;

	/**
	 * Stores whether the initial exhaustion of memory buffers has already been reported
	 */
	private boolean initialExhaustionOfMemoryBuffersReported = false;

	RuntimeTaskContext(final RuntimeTask task, final TransferEnvelopeDispatcher transferEnvelopeDispatcher,
			final Map<ExecutionVertexID, RuntimeTaskContext> tasksWithUndecidedCheckpoints) {

		this.localBufferPool = new LocalBufferPool(1, false, this);
		this.task = task;

		final RuntimeEnvironment environment = task.getRuntimeEnvironment();

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

		this.ephemeralCheckpoint = new EphemeralCheckpoint(task, ephemeral);
		if (ephemeral) {
			tasksWithUndecidedCheckpoints.put(task.getVertexID(), this);
		}

		this.transferEnvelopeDispatcher = transferEnvelopeDispatcher;
		this.runtimeDispatcher = new RuntimeDispatcher(transferEnvelopeDispatcher);
	}

	RuntimeDispatcher getRuntimeDispatcher() {

		return this.runtimeDispatcher;
	}

	EphemeralCheckpoint getEphemeralCheckpoint() {

		return this.ephemeralCheckpoint;
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

		final RuntimeEnvironment environment = this.task.getRuntimeEnvironment();

		System.out.println("\t\t" + environment.getTaskName() + ": " + ava + " available, " + req + " requested, "
			+ des + " designated");
	}

	/**
	 * Called by an {@link OutputGateContext} to indicate that the task has temporarily run out of memory buffers.
	 */
	void reportExhaustionOfMemoryBuffers() {

		if (!this.initialExhaustionOfMemoryBuffersReported) {

			System.out.println(this.task.getEnvironment().getTaskName() + " has buffers exhausted");
			
			this.task.initialExecutionResourcesExhausted();
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

		// Check if the checkpoint decision changed
		this.ephemeralCheckpoint.checkAsynchronousCheckpointDecision();
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

	public void setCheckpointDecisionAsynchronously(final boolean checkpointDecision) {

		// Simply delegate call
		this.ephemeralCheckpoint.setCheckpointDecisionAsynchronously(checkpointDecision);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public OutputGateContext createOutputGateContext(final GateID gateID) {

		if (gateID == null) {
			throw new IllegalArgumentException("Argument gateID must not be null");
		}

		OutputGate<? extends Record> outputGate = null;
		final RuntimeEnvironment re = this.task.getRuntimeEnvironment();
		for (int i = 0; i < re.getNumberOfOutputGates(); ++i) {
			final OutputGate<? extends Record> candidateGate = re.getOutputGate(i);
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

		InputGate<? extends Record> inputGate = null;
		final RuntimeEnvironment re = this.task.getRuntimeEnvironment();
		for (int i = 0; i < re.getNumberOfInputGates(); ++i) {
			final InputGate<? extends Record> candidateGate = re.getInputGate(i);
			if (candidateGate.getGateID().equals(gateID)) {
				inputGate = candidateGate;
				break;
			}
		}

		if (inputGate == null) {
			throw new IllegalStateException("Cannot find input gate with ID " + gateID);
		}

		return new RuntimeInputGateContext(this.transferEnvelopeDispatcher, inputGate);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public LocalBufferPoolOwner getLocalBufferPoolOwner() {

		return this;
	}
}
