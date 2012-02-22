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
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.annotations.ForceCheckpoint;
import eu.stratosphere.nephele.annotations.Stateful;
import eu.stratosphere.nephele.annotations.Stateless;
import eu.stratosphere.nephele.checkpointing.EphemeralCheckpoint;
import eu.stratosphere.nephele.execution.ResourceUtilizationSnapshot;
import eu.stratosphere.nephele.execution.RuntimeEnvironment;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.AbstractID;
import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.channels.AbstractInputChannel;
import eu.stratosphere.nephele.io.channels.AbstractOutputChannel;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.taskmanager.bufferprovider.AsynchronousEventListener;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferProvider;
import eu.stratosphere.nephele.taskmanager.bufferprovider.LocalBufferPool;
import eu.stratosphere.nephele.taskmanager.bytebuffered.InputGateContext;
import eu.stratosphere.nephele.taskmanager.bytebuffered.OutputGateContext;
import eu.stratosphere.nephele.taskmanager.bytebuffered.TaskContext;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelopeDispatcher;
import eu.stratosphere.nephele.types.Record;

public final class RuntimeTaskContext implements BufferProvider, AsynchronousEventListener, TaskContext {

	/**
	 * The log object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(RuntimeTaskContext.class);

	private static final long NANO_TO_MILLISECONDS = 1000 * 1000;

	private final LocalBufferPool localBufferPool;

	private final RuntimeTask task;

	private final int numberOfOutputChannels;

	private final TransferEnvelopeDispatcher transferEnvelopeDispatcher;

	private final RuntimeDispatcher runtimeDispatcher;

	private final EphemeralCheckpoint ephemeralCheckpoint;

	private final EnvelopeConsumptionLog envelopeConsumptionLog;

	private final long startTime;

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

		this.envelopeConsumptionLog = new EnvelopeConsumptionLog(task.getVertexID(), environment);

		this.startTime = System.currentTimeMillis();
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
	void reportExhaustionOfMemoryBuffers() {

		if (!this.initialExhaustionOfMemoryBuffersReported) {

			this.initialExhaustionOfMemoryBuffersReported = true;

			final RuntimeEnvironment environment = this.task.getRuntimeEnvironment();

			System.out.println("PACT input/output for task " + environment.getTaskNameWithIndex() + ": "
				+ this.task.getPACTInputOutputRatio());

			// if (this.environment.getExecutingThread() != Thread.currentThread()) {
			// throw new ConcurrentModificationException(
			// "initialExecutionResourcesExhausted must be called from the task that executes the user code");
			// }

			// Construct a resource utilization snapshot
			final long timestamp = System.currentTimeMillis();
			if (environment.getInputGate(0) != null
				&& environment.getInputGate(0).getExecutionStart() < timestamp) {
				this.startTime = environment.getInputGate(0).getExecutionStart();
			}
			LOG.info("Task " + environment.getTaskNameWithIndex() + " started " + this.startTime);
			// Get CPU-Usertime in percent
			ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
			long userCPU = (threadBean.getCurrentThreadUserTime() / NANO_TO_MILLISECONDS) * 100
				/ (timestamp - this.startTime);
			LOG.info("USER CPU for " + environment.getTaskNameWithIndex() + " : " + userCPU);
			// collect outputChannelUtilization
			final Map<ChannelID, Long> channelUtilization = new HashMap<ChannelID, Long>();
			long totalOutputAmount = 0;
			int numrec = 0;
			long averageOutputRecordSize = 0;
			for (int i = 0; i < environment.getNumberOfOutputGates(); ++i) {
				final OutputGate<? extends Record> outputGate = environment.getOutputGate(i);
				numrec += outputGate.getNumRecords();
				for (int j = 0; j < outputGate.getNumberOfOutputChannels(); ++j) {
					final AbstractOutputChannel<? extends Record> outputChannel = outputGate.getOutputChannel(j);
					channelUtilization.put(outputChannel.getID(),
						Long.valueOf(outputChannel.getAmountOfDataTransmitted()));
					totalOutputAmount += outputChannel.getAmountOfDataTransmitted();
				}
			}

			if (numrec != 0) {
				averageOutputRecordSize = totalOutputAmount / numrec;
			}
			// FIXME (marrus) it is not about what we received but what we processed yet
			boolean allClosed = true;
			int numinrec = 0;

			long totalInputAmount = 0;
			long averageInputRecordSize = 0;
			for (int i = 0; i < environment.getNumberOfInputGates(); ++i) {
				final InputGate<? extends Record> inputGate = environment.getInputGate(i);
				numinrec += inputGate.getNumRecords();
				for (int j = 0; j < inputGate.getNumberOfInputChannels(); ++j) {
					final AbstractInputChannel<? extends Record> inputChannel = inputGate.getInputChannel(j);
					channelUtilization.put(inputChannel.getID(),
						Long.valueOf(inputChannel.getAmountOfDataTransmitted()));
					totalInputAmount += inputChannel.getAmountOfDataTransmitted();
					try {
						if (!inputChannel.isClosed()) {
							allClosed = false;
						}
					} catch (IOException e) {
						e.printStackTrace();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}

				}
			}
			if (numinrec != 0) {
				averageInputRecordSize = totalInputAmount / numinrec;
			}
			Boolean force = null;
			Boolean stateful = false;
			if (environment.getInvokable().getClass().isAnnotationPresent(Stateful.class)
				&& !environment.getInvokable().getClass().isAnnotationPresent(Stateless.class)) {
				// Don't checkpoint stateful tasks
				force = false;
			} else {
				if (environment.getForced() != null) {
					force = environment.getForced();
				} else {
					// look for a forced decision from the user
					ForceCheckpoint forced = environment.getInvokable().getClass().getAnnotation(ForceCheckpoint.class);

					// this.environment.getInvokable().getTaskConfiguration().getBoolean("forced_checkpoint", false)

					if (forced != null) {
						force = forced.checkpoint();
					}
				}
			}

			final ResourceUtilizationSnapshot rus = new ResourceUtilizationSnapshot(timestamp, channelUtilization,
				userCPU,
				force, totalInputAmount, totalOutputAmount, averageOutputRecordSize, averageInputRecordSize,
				this.task.getPACTInputOutputRatio(), allClosed);

			
			// Finally, propagate event to the job manager
			this.taskManager.initialExecutionResourcesExhausted(this.environment.getJobID(), this.vertexID, rus);

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

		return new RuntimeInputGateContext(re.getTaskNameWithIndex(), this.transferEnvelopeDispatcher, inputGate,
			this.envelopeConsumptionLog);
	}

	public LocalBufferPool getLocalBufferPool() {

		return this.localBufferPool;
	}
}
