package eu.stratosphere.nephele.checkpointing;

import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.taskmanager.bytebuffered.AbstractOutputChannelContext;
import eu.stratosphere.nephele.taskmanager.bytebuffered.IncomingEventQueue;
import eu.stratosphere.nephele.taskmanager.bytebuffered.OutputChannelContext;
import eu.stratosphere.nephele.taskmanager.bytebuffered.OutputChannelForwardingChain;
import eu.stratosphere.nephele.taskmanager.bytebuffered.OutputGateContext;
import eu.stratosphere.nephele.taskmanager.bytebuffered.UnexpectedEnvelopeEvent;
import eu.stratosphere.nephele.taskmanager.runtime.ForwardingBarrier;
import eu.stratosphere.nephele.taskmanager.runtime.SpillingBarrier;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;

final class ReplayOutputGateContext extends AbstractReplayGateContext implements OutputGateContext {

	private final ReplayTaskContext taskContext;

	ReplayOutputGateContext(final ReplayTaskContext taskContext, final GateID gateID) {
		super(gateID);

		this.taskContext = taskContext;
	}

	@Override
	public OutputChannelContext createOutputChannelContext(ChannelID channelID, OutputChannelContext previousContext,
			boolean isReceiverRunning, boolean mergeSpillBuffers) {

		if (previousContext != null) {
			activateForwardingBarrier(previousContext);
		}

		// Construct new forwarding chain for the replay output channel context
		final OutputChannelForwardingChain forwardingChain = new OutputChannelForwardingChain();
		final IncomingEventQueue incomingEventQueue = AbstractOutputChannelContext
			.createIncomingEventQueue(forwardingChain);
		final ReplayOutputBroker outputBroker = new ReplayOutputBroker(this.taskContext, forwardingChain,
			incomingEventQueue);
		forwardingChain.addForwarder(outputBroker);
		forwardingChain.addForwarder(new ForwardingBarrier(channelID));
		forwardingChain.addForwarder(new SpillingBarrier(isReceiverRunning, mergeSpillBuffers));
		forwardingChain.addForwarder(this.taskContext.getRuntimeDispatcher());

		// Register output broker
		this.taskContext.registerReplayOutputBroker(channelID, outputBroker);

		return new ReplayOutputChannelContext(null, channelID, incomingEventQueue, previousContext);
	}

	private static void activateForwardingBarrier(final OutputChannelContext previousContext) {

		final TransferEnvelope transferEnvelope = new TransferEnvelope(0, previousContext.getJobID(),
			previousContext.getConnectedChannelID());

		transferEnvelope.addEvent(new UnexpectedEnvelopeEvent(Integer.MAX_VALUE));
		previousContext.queueTransferEnvelope(transferEnvelope);
	}

}
