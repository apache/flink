package eu.stratosphere.nephele.checkpointing;

import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.taskmanager.bufferprovider.LocalBufferPoolOwner;
import eu.stratosphere.nephele.taskmanager.bytebuffered.InputGateContext;
import eu.stratosphere.nephele.taskmanager.bytebuffered.OutputGateContext;
import eu.stratosphere.nephele.taskmanager.bytebuffered.TaskContext;
import eu.stratosphere.nephele.taskmanager.runtime.RuntimeDispatcher;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelopeDispatcher;

final class ReplayTaskContext implements TaskContext {

	private final ReplayTask task;

	private final RuntimeDispatcher runtimeDispatcher;

	ReplayTaskContext(final ReplayTask task, final TransferEnvelopeDispatcher transferEnvelopeDispatcher) {
		this.task = task;
		this.runtimeDispatcher = new RuntimeDispatcher(transferEnvelopeDispatcher);
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
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public LocalBufferPoolOwner getLocalBufferPoolOwner() {

		return null;
	}

	void registerReplayOutputBroker(final ChannelID channelID, final ReplayOutputBroker outputBroker) {

		this.task.registerReplayOutputBroker(channelID, outputBroker);
	}

	RuntimeDispatcher getRuntimeDispatcher() {

		return this.runtimeDispatcher;
	}
}
