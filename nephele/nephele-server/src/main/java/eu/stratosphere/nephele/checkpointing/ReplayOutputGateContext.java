package eu.stratosphere.nephele.checkpointing;

import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.taskmanager.bytebuffered.OutputChannelContext;
import eu.stratosphere.nephele.taskmanager.bytebuffered.OutputGateContext;
import eu.stratosphere.nephele.taskmanager.bytebuffered.UnexpectedEnvelopeEvent;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;

final class ReplayOutputGateContext extends AbstractReplayGateContext implements OutputGateContext {

	ReplayOutputGateContext(final GateID gateID) {
		super(gateID);
	}

	@Override
	public OutputChannelContext createOutputChannelContext(ChannelID channelID, OutputChannelContext previousContext,
			boolean isReceiverRunning, boolean mergeSpillBuffers) {

		if (previousContext != null) {
			activateForwardingBarrier(previousContext);
		}

		return new ReplayOutputChannelContext(channelID, previousContext);
	}

	private static void activateForwardingBarrier(final OutputChannelContext previousContext) {

		final TransferEnvelope transferEnvelope = new TransferEnvelope(0, previousContext.getJobID(),
			previousContext.getConnectedChannelID());

		transferEnvelope.addEvent(new UnexpectedEnvelopeEvent(Integer.MAX_VALUE));
		previousContext.queueTransferEnvelope(transferEnvelope);
	}

}
