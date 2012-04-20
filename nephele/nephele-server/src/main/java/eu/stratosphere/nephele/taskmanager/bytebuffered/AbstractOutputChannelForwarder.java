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

package eu.stratosphere.nephele.taskmanager.bytebuffered;

import java.io.IOException;

import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedOutputChannel;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;

/**
 * An output channel forwarder is a component which processes a {@link TransferEnvelope} after it has been produced by
 * an {@link AbstractByteBufferedOutputChannel}. The component can decide based on the transfer envelope whether to
 * forward the envelope, discard it, or to store it.
 * 
 * @author warneke
 */
public abstract class AbstractOutputChannelForwarder {

	private final AbstractOutputChannelForwarder next;

	private volatile AbstractOutputChannelForwarder prev = null;

	protected AbstractOutputChannelForwarder(final AbstractOutputChannelForwarder next) {
		this.next = next;
		if (this.next != null) {
			this.next.prev = this;
		}
	}

	/**
	 * Called by the framework to push a produced transfer envelope towards its receiver. This method will always be
	 * called by the task thread itself.
	 * 
	 * @param transferEnvelope
	 *        the transfer envelope to be processed
	 * @throws IOException
	 *         thrown if an I/O error occurs while processing the transfer envelope
	 * @throws InterruptedException
	 *         thrown if the task thread was interrupted while processing the transfer envelope
	 */
	public void push(final TransferEnvelope transferEnvelope) throws IOException, InterruptedException {

		if (this.next != null) {
			this.next.push(transferEnvelope);
		}
	}

	public TransferEnvelope pull() {

		if (this.prev != null) {
			return this.prev.pull();
		}

		return null;
	}

	public boolean hasDataLeft() throws IOException, InterruptedException {

		if (this.next != null) {
			this.next.hasDataLeft();
		}

		return false;
	}

	public void processEvent(final AbstractEvent event) {

		if (this.next != null) {
			this.next.processEvent(event);
		}
	}

	public void destroy() {

		if (this.next != null) {
			this.next.destroy();
		}
	}

	protected final void recycleTransferEnvelope(final TransferEnvelope transferEnvelope) {

		final Buffer buffer = transferEnvelope.getBuffer();
		if (buffer != null) {
			buffer.recycleBuffer();

		}
	}

	protected final AbstractOutputChannelForwarder getNext() {

		return this.next;
	}

	protected final AbstractOutputChannelForwarder getPrev() {

		return this.prev;
	}
}
