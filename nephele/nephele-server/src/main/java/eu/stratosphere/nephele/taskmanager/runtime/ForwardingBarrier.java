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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.taskmanager.bytebuffered.OutputChannelForwarder;
import eu.stratosphere.nephele.taskmanager.bytebuffered.UnexpectedEnvelopeEvent;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;

public final class ForwardingBarrier implements OutputChannelForwarder {

	private static final Log LOG = LogFactory.getLog(ForwardingBarrier.class);

	private final ChannelID outputChannelID;

	private int forwardingBarrier = -1;

	ForwardingBarrier(final ChannelID outputChannelID) {
		this.outputChannelID = outputChannelID;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean forward(final TransferEnvelope transferEnvelope) {

		if (transferEnvelope.getSequenceNumber() < this.forwardingBarrier) {
			return false;
		}
		
		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean hasDataLeft() {

		return false;
	}

	@Override
	public void processEvent(final AbstractEvent event) {

		if (event instanceof UnexpectedEnvelopeEvent) {

			final UnexpectedEnvelopeEvent uee = (UnexpectedEnvelopeEvent) event;
			this.forwardingBarrier = uee.getExpectedSequenceNumber();
			LOG.info("Setting forwarding barrier to sequence number " + this.forwardingBarrier + " for output channel " + this.outputChannelID);
		}
	}
}
