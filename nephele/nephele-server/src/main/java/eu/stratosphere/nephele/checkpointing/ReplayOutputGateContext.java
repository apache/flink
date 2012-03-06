package eu.stratosphere.nephele.checkpointing;

import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.taskmanager.bytebuffered.OutputChannelContext;
import eu.stratosphere.nephele.taskmanager.bytebuffered.OutputChannelForwardingChain;
import eu.stratosphere.nephele.taskmanager.bytebuffered.OutputGateContext;
import eu.stratosphere.nephele.taskmanager.bytebuffered.UnexpectedEnvelopeEvent;
import eu.stratosphere.nephele.taskmanager.runtime.ForwardingBarrier;
import eu.stratosphere.nephele.taskmanager.runtime.RuntimeDispatcher;
import eu.stratosphere.nephele.taskmanager.runtime.SpillingBarrier;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;

final class ReplayOutputGateContext extends AbstractReplayGateContext implements OutputGateContext {

	private final ReplayTaskContext taskContext;

	ReplayOutputGateContext(final ReplayTaskContext taskContext, final GateID gateID) {
		super(gateID);

		this.taskContext = taskContext;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public OutputChannelContext createOutputChannelContext(ChannelID channelID, OutputChannelContext previousContext,
			boolean isReceiverRunning, boolean mergeSpillBuffers) {

		if (previousContext != null) {
			activateForwardingBarrier(previousContext);
		}

		// Construct new forwarding chain for the replay output channel context
		final RuntimeDispatcher runtimeDispatcher = new RuntimeDispatcher(
			this.taskContext.getTransferEnvelopeDispatcher());
		final SpillingBarrier spillingBarrier = new SpillingBarrier(isReceiverRunning, mergeSpillBuffers,
			runtimeDispatcher);
		final ForwardingBarrier forwardingBarrier = new ForwardingBarrier(channelID, spillingBarrier);
		final ReplayOutputBroker outputChannelBroker = new ReplayOutputBroker(this.taskContext, forwardingBarrier);

		final OutputChannelForwardingChain forwardingChain = new OutputChannelForwardingChain(outputChannelBroker,
			runtimeDispatcher);

		// Set forwarding chain for broker
		outputChannelBroker.setForwardingChain(forwardingChain);

		// Register output broker
		this.taskContext.registerReplayOutputBroker(channelID, outputChannelBroker);

		return new ReplayOutputChannelContext(null, channelID, forwardingChain, previousContext);
	}

	private static void activateForwardingBarrier(final OutputChannelContext previousContext) {

		final TransferEnvelope transferEnvelope = new TransferEnvelope(0, previousContext.getJobID(),
			previousContext.getConnectedChannelID());

		transferEnvelope.addEvent(new UnexpectedEnvelopeEvent(Integer.MAX_VALUE));
		previousContext.queueTransferEnvelope(transferEnvelope);
	}

}
