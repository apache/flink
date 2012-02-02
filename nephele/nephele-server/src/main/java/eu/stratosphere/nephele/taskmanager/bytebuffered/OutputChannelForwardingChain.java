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
import java.util.Iterator;
import java.util.concurrent.CopyOnWriteArrayList;

import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;

public final class OutputChannelForwardingChain {

	private final CopyOnWriteArrayList<OutputChannelForwarder> forwardingChain = new CopyOnWriteArrayList<OutputChannelForwarder>();

	public void addForwarder(final OutputChannelForwarder forwarder) {

		this.forwardingChain.add(forwarder);
	}

	public void forwardEnvelope(final TransferEnvelope transferEnvelope) throws IOException, InterruptedException {

		final Iterator<OutputChannelForwarder> it = this.forwardingChain.iterator();
		while (it.hasNext()) {

			if (!it.next().forward(transferEnvelope)) {
				recycleEnvelope(transferEnvelope);
				break;
			}

		}
	}

	public void processEvent(final AbstractEvent event) {

		final Iterator<OutputChannelForwarder> it = this.forwardingChain.iterator();
		while (it.hasNext()) {
			it.next().processEvent(event);
		}
	}

	public boolean anyForwarderHasDataLeft() {

		final Iterator<OutputChannelForwarder> it = this.forwardingChain.iterator();
		while (it.hasNext()) {

			if (it.next().hasDataLeft()) {
				return true;
			}
		}

		return false;
	}

	private void recycleEnvelope(final TransferEnvelope transferEnvelope) {

		final Buffer buffer = transferEnvelope.getBuffer();
		if (buffer != null) {
			buffer.recycleBuffer();
		}
	}

}
