/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.checkpointing;

import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.taskmanager.routing.OutputChannelContext;
import eu.stratosphere.nephele.taskmanager.routing.OutputChannelForwardingChain;
import eu.stratosphere.nephele.taskmanager.routing.OutputGateContext;
import eu.stratosphere.nephele.taskmanager.routing.UnexpectedEnvelopeEvent;
import eu.stratosphere.nephele.taskmanager.runtime.ForwardingBarrier;
import eu.stratosphere.nephele.taskmanager.runtime.RuntimeDispatcher;
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
			this.taskContext.getRoutingLayer());
		/*
		 * final SpillingBarrier spillingBarrier = new SpillingBarrier(isReceiverRunning, mergeSpillBuffers,
		 * runtimeDispatcher);
		 * final ForwardingBarrier forwardingBarrier = new ForwardingBarrier(channelID, spillingBarrier);
		 */
		final ForwardingBarrier forwardingBarrier = new ForwardingBarrier(channelID, runtimeDispatcher);
		final ReplayOutputChannelBroker outputChannelBroker = new ReplayOutputChannelBroker(this.taskContext,
			forwardingBarrier);

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
