package eu.stratosphere.nephele.checkpointing;

import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.taskmanager.bytebuffered.AbstractOutputChannelContext;
import eu.stratosphere.nephele.taskmanager.bytebuffered.OutputChannelContext;
import eu.stratosphere.nephele.taskmanager.bytebuffered.OutputChannelForwardingChain;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;

public final class ReplayOutputChannelContext extends AbstractOutputChannelContext implements OutputChannelContext {

	private final JobID jobID;

	private final ChannelID channelID;

	private final OutputChannelContext encapsulatedContext;

	ReplayOutputChannelContext(final JobID jobID, final ChannelID channelID,
			final OutputChannelForwardingChain forwardingChain, final OutputChannelContext encapsulatedContext) {
		super(forwardingChain);

		this.jobID = jobID;
		this.channelID = channelID;
		this.encapsulatedContext = encapsulatedContext;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isInputChannel() {

		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public JobID getJobID() {

		return this.jobID;
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

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void queueTransferEnvelope(final TransferEnvelope transferEnvelope) {

		super.queueTransferEnvelope(transferEnvelope);

		if (this.encapsulatedContext != null) {
			this.encapsulatedContext.queueTransferEnvelope(transferEnvelope);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void destroy() {

		super.destroy();

		if (this.encapsulatedContext != null) {
			this.encapsulatedContext.destroy();
		}
	}
}
