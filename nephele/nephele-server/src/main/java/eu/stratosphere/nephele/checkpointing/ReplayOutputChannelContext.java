package eu.stratosphere.nephele.checkpointing;

import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.taskmanager.bytebuffered.OutputChannelContext;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;

public final class ReplayOutputChannelContext implements OutputChannelContext {

	private final ChannelID channelID;
	
	private final OutputChannelContext encapsulatedContext;

	ReplayOutputChannelContext(final ChannelID channelID, final OutputChannelContext encapsulatedContext) {
		
		this.channelID = channelID;
		this.encapsulatedContext = encapsulatedContext;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isInputChannel() {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public JobID getJobID() {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ChannelID getChannelID() {
		
		return this.channelID;
	}

	@Override
	public ChannelID getConnectedChannelID() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelType getType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void queueTransferEnvelope(TransferEnvelope transferEnvelope) {
		// TODO Auto-generated method stub

	}

	@Override
	public void releaseAllResources() {
		// TODO Auto-generated method stub
		
	}

}
