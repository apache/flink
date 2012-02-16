package eu.stratosphere.nephele.checkpointing;

import java.io.IOException;

import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.taskmanager.bufferprovider.AsynchronousEventListener;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferProvider;
import eu.stratosphere.nephele.taskmanager.bufferprovider.LocalBufferPool;
import eu.stratosphere.nephele.taskmanager.bufferprovider.LocalBufferPoolOwner;
import eu.stratosphere.nephele.taskmanager.bytebuffered.InputGateContext;
import eu.stratosphere.nephele.taskmanager.bytebuffered.OutputGateContext;
import eu.stratosphere.nephele.taskmanager.bytebuffered.TaskContext;
import eu.stratosphere.nephele.taskmanager.runtime.RuntimeDispatcher;
import eu.stratosphere.nephele.taskmanager.runtime.RuntimeTaskContext;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelopeDispatcher;

final class ReplayTaskContext implements TaskContext, BufferProvider, AsynchronousEventListener {

	private final ReplayTask task;

	private final RuntimeDispatcher runtimeDispatcher;

	private final int numberOfChannels;

	private final LocalBufferPool localBufferPool;

	ReplayTaskContext(final ReplayTask task, final TransferEnvelopeDispatcher transferEnvelopeDispatcher,
			final LocalBufferPoolOwner previousBufferPoolOwner, final int numberOfChannels) {
		this.task = task;
		this.runtimeDispatcher = new RuntimeDispatcher(transferEnvelopeDispatcher);
		if (previousBufferPoolOwner == null) {
			this.localBufferPool = new LocalBufferPool(1, false, this);
		} else {
			if (!(previousBufferPoolOwner instanceof RuntimeTaskContext)) {
				throw new IllegalStateException("previousBufferPoolOwner is not of type RuntimeTaskContext");
			}

			final RuntimeTaskContext rtc = (RuntimeTaskContext) previousBufferPoolOwner;
			this.localBufferPool = rtc.getLocalBufferPool();
		}
		this.numberOfChannels = numberOfChannels;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public OutputGateContext createOutputGateContext(final GateID gateID) {

		return new ReplayOutputGateContext(this, gateID);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public InputGateContext createInputGateContext(final GateID gateID) {

		return new ReplayInputGateContext(gateID);
	}

	void registerReplayOutputBroker(final ChannelID channelID, final ReplayOutputBroker outputBroker) {

		this.task.registerReplayOutputBroker(channelID, outputBroker);
	}

	RuntimeDispatcher getRuntimeDispatcher() {

		return this.runtimeDispatcher;
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
	public Buffer requestEmptyBufferBlocking(final int minimumSizeOfBuffer) throws IOException, InterruptedException {

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
	public boolean isShared() {

		return false;
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

		return this.numberOfChannels;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setDesignatedNumberOfBuffers(final int numberOfBuffers) {

		this.localBufferPool.setDesignatedNumberOfBuffers(numberOfBuffers);
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
	public void logBufferUtilization() {

		final int ava = this.localBufferPool.getNumberOfAvailableBuffers();
		final int req = this.localBufferPool.getRequestedNumberOfBuffers();
		final int des = this.localBufferPool.getDesignatedNumberOfBuffers();

		final Environment environment = this.task.getEnvironment();

		System.out.println("\t\t" + environment.getTaskName() + " (Replay): " + ava + " available, " + req
			+ " requested, " + des + " designated");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void asynchronousEventOccurred() throws IOException, InterruptedException {

		throw new IllegalStateException("ReplayTaskContext received asynchronous event");
	}
}
