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

import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.taskmanager.bufferprovider.LocalBufferPoolOwner;
import eu.stratosphere.nephele.taskmanager.bytebuffered.InputChannelContext;
import eu.stratosphere.nephele.taskmanager.bytebuffered.InputGateContext;

final class ReplayInputGateContext extends AbstractReplayGateContext implements InputGateContext {

	/**
	 * Constructs a new replay input gate context.
	 * 
	 * @param gateID
	 *        the ID of the gate this context is created for
	 */
	ReplayInputGateContext(final GateID gateID) {
		super(gateID);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public InputChannelContext createInputChannelContext(ChannelID channelID, InputChannelContext previousContext) {

		return new ReplayInputChannelContext(previousContext);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public LocalBufferPoolOwner getLocalBufferPoolOwner() {

		return null;
	}

}
