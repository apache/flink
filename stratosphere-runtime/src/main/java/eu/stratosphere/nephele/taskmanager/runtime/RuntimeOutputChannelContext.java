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

import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedOutputChannel;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.taskmanager.bytebuffered.AbstractOutputChannelContext;
import eu.stratosphere.nephele.taskmanager.bytebuffered.OutputChannelForwardingChain;

public final class RuntimeOutputChannelContext extends AbstractOutputChannelContext {

	private final AbstractByteBufferedOutputChannel<?> byteBufferedOutputChannel;

	RuntimeOutputChannelContext(final AbstractByteBufferedOutputChannel<?> byteBufferedOutputChannel,
			final OutputChannelForwardingChain forwardingChain) {
		super(forwardingChain);

		this.byteBufferedOutputChannel = byteBufferedOutputChannel;
	}


	@Override
	public boolean isInputChannel() {

		return false;
	}


	@Override
	public ChannelID getChannelID() {

		return this.byteBufferedOutputChannel.getID();
	}


	@Override
	public ChannelID getConnectedChannelID() {

		return this.byteBufferedOutputChannel.getConnectedChannelID();
	}


	@Override
	public JobID getJobID() {

		return this.byteBufferedOutputChannel.getJobID();
	}


	@Override
	public ChannelType getType() {

		return this.byteBufferedOutputChannel.getType();
	}


	@Override
	protected void processEventAsynchronously(final AbstractEvent event) {

		this.byteBufferedOutputChannel.processEvent(event);
	}
}
