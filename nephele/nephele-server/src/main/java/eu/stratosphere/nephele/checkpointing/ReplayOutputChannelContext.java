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

package eu.stratosphere.nephele.checkpointing;

import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.taskmanager.bytebuffered.AbstractOutputChannelContext;
import eu.stratosphere.nephele.taskmanager.bytebuffered.OutputChannelContext;
import eu.stratosphere.nephele.taskmanager.bytebuffered.OutputChannelForwardingChain;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;

/**
 * The class implements an {@link OutputChannelContext} for a replay task.
 * 
 * @author warneke
 */
public final class ReplayOutputChannelContext extends AbstractOutputChannelContext implements OutputChannelContext {

	private final JobID jobID;

	private final ChannelID channelID;

	private final OutputChannelContext encapsulatedContext;

	ReplayOutputChannelContext(final JobID jobID, final ChannelID channelID,
			final OutputChannelForwardingChain forwardingChain, final OutputChannelContext encapsulatedContext) {
		super(forwardingChain);

		this.jobID = jobID;
		this.channelID = channelID;
		this.encapsulatedContext = encapsulatedContext;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isInputChannel() {

		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public JobID getJobID() {

		return this.jobID;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ChannelID getChannelID() {

		return this.channelID;
	}

	@Override
	public ChannelID getConnectedChannelID() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelType getType() {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void queueTransferEnvelope(final TransferEnvelope transferEnvelope) {

		super.queueTransferEnvelope(transferEnvelope);

		if (this.encapsulatedContext != null) {
			this.encapsulatedContext.queueTransferEnvelope(transferEnvelope);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void destroy() {

		super.destroy();

		if (this.encapsulatedContext != null) {
			this.encapsulatedContext.destroy();
		}
	}
}
