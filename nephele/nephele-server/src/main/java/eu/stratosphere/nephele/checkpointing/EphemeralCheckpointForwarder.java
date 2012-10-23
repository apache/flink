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

import java.io.IOException;

import eu.stratosphere.nephele.taskmanager.bytebuffered.AbstractOutputChannelForwarder;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;

public final class EphemeralCheckpointForwarder extends AbstractOutputChannelForwarder {

	private final EphemeralCheckpoint ephemeralCheckpoint;

	public EphemeralCheckpointForwarder(final EphemeralCheckpoint ephemeralCheckpoint,
			final AbstractOutputChannelForwarder next) {
		super(next);

		this.ephemeralCheckpoint = ephemeralCheckpoint;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void push(final TransferEnvelope transferEnvelope) throws IOException, InterruptedException {

		this.ephemeralCheckpoint.forward(transferEnvelope);

		final AbstractOutputChannelForwarder next = getNext();
		if (next != null) {
			next.push(transferEnvelope);
		} else {
			recycleTransferEnvelope(transferEnvelope);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean hasDataLeft() throws IOException, InterruptedException {

		if (this.ephemeralCheckpoint.hasDataLeft()) {
			return true;
		}

		final AbstractOutputChannelForwarder next = getNext();
		if (next != null) {
			return getNext().hasDataLeft();
		}

		return false;
	}
	
	@Override
	public void destroy() {
		
		this.ephemeralCheckpoint.destroy();
		
		super.destroy();
	}
}
