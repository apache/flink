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

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;

import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;

public final class OutputChannelForwardingChain {

	private final Queue<AbstractEvent> incomingEventQueue = new LinkedBlockingDeque<AbstractEvent>();

	private final AbstractOutputChannelForwarder first;

	private final AbstractOutputChannelForwarder last;

	public OutputChannelForwardingChain(final AbstractOutputChannelForwarder first,
			final AbstractOutputChannelForwarder last) {

		if (first == null) {
			throw new IllegalArgumentException("Argument first must not be null");
		}

		if (last == null) {
			throw new IllegalArgumentException("Argument last must not be null");
		}

		this.first = first;
		this.last = last;
	}

	public void pushEnvelope(final TransferEnvelope transferEnvelope) throws IOException, InterruptedException {

		this.first.push(transferEnvelope);
	}

	public TransferEnvelope pullEnvelope() {

		return this.last.pull();
	}

	public void processEvent(final AbstractEvent event) {

		this.first.processEvent(event);
	}

	public boolean anyForwarderHasDataLeft() throws IOException, InterruptedException {

		return this.first.hasDataLeft();
	}

	public void destroy() {

		this.first.destroy();
	}

	public void processQueuedEvents() {

		AbstractEvent event = this.incomingEventQueue.poll();
		while (event != null) {

			this.first.processEvent(event);
			event = this.incomingEventQueue.poll();
		}
	}

	void offerEvent(final AbstractEvent event) {
		this.incomingEventQueue.offer(event);
	}
}
