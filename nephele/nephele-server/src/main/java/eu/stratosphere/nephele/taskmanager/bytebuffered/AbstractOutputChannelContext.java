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

package eu.stratosphere.nephele.taskmanager.bytebuffered;

import java.util.Iterator;

import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;

public abstract class AbstractOutputChannelContext implements OutputChannelContext {

	/**
	 * The forwarding chain used by this output channel context.
	 */
	private final OutputChannelForwardingChain forwardingChain;

	public AbstractOutputChannelContext(final OutputChannelForwardingChain forwardingChain) {

		this.forwardingChain = forwardingChain;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void queueTransferEnvelope(final TransferEnvelope transferEnvelope) {

		if (transferEnvelope.getBuffer() != null) {
			throw new IllegalStateException("Transfer envelope for output channel has buffer attached");
		}

		final Iterator<AbstractEvent> it = transferEnvelope.getEventList().iterator();
		while (it.hasNext()) {

			final AbstractEvent event = it.next();
			if (event instanceof AbstractTaskEvent) {
				processEventAsynchronously(event);
			} else {
				processEventSynchronously(event);
			}
		}
	}

	/**
	 * Processes an event received from the framework in a synchronous fashion, i.e. the event processing is done by the
	 * thread the event is destined for (usually the task thread).
	 * 
	 * @param event
	 *        the event to be processed
	 */
	protected void processEventSynchronously(final AbstractEvent event) {

		this.forwardingChain.offerEvent(event);
	}

	/**
	 * Processes an event received from the framework in an asynchronous fashion, i.e. the event processing is done by
	 * the thread which delivers the event.
	 * 
	 * @param event
	 *        the event to be processed
	 */
	protected void processEventAsynchronously(final AbstractEvent event) {

		// The default implementation does nothing
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void destroy() {

		this.forwardingChain.destroy();
	}
}
