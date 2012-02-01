package eu.stratosphere.nephele.checkpointing;

import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.taskmanager.bufferprovider.LocalBufferPoolOwner;
import eu.stratosphere.nephele.taskmanager.bytebuffered.InputChannelContext;
import eu.stratosphere.nephele.taskmanager.bytebuffered.InputGateContext;

final class ReplayInputGateContext extends AbstractReplayGateContext implements InputGateContext {

	ReplayInputGateContext(final GateID gateID) {
		super(gateID);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public InputChannelContext createInputChannelContext(ChannelID channelID, InputChannelContext previousContext) {

		return new ReplayInputChannelContext(previousContext);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public LocalBufferPoolOwner getLocalBufferPoolOwner() {
		// TODO Auto-generated method stub
		return null;
	}

}
