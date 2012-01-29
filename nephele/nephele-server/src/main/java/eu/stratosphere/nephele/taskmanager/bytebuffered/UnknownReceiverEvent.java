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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.io.channels.ChannelID;

/**
 * An unknown receiver event can be used by the framework to inform a sender task that the delivery of a
 * {@link TransferEnvelope} has failed since the receiver could not be found.
 * 
 * @author warneke
 */
public final class UnknownReceiverEvent extends AbstractEvent {

	/**
	 * The ID of the unknown receiver.
	 */
	private ChannelID unknownReceiverID;

	/**
	 * Constructs a new unknown receiver event.
	 * 
	 * @param unknownReceiverID
	 *        the ID of the unknown receiver
	 */
	public UnknownReceiverEvent(final ChannelID unknownReceiverID) {

		if (unknownReceiverID == null) {
			throw new IllegalArgumentException("Argument unknownReceiverID must not be null");
		}

		this.unknownReceiverID = unknownReceiverID;
	}

	/**
	 * Default constructor for serialization/deserialization.
	 */
	public UnknownReceiverEvent() {

		this.unknownReceiverID = new ChannelID();
	}

	/**
	 * Returns the ID of the unknown receiver.
	 * 
	 * @return the ID of the unknown receiver
	 */
	public ChannelID getUnknownReceiverID() {

		return this.unknownReceiverID;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {

		this.unknownReceiverID.write(out);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {

		this.unknownReceiverID.read(in);
	}

}
