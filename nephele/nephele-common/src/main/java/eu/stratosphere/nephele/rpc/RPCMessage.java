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

package eu.stratosphere.nephele.rpc;

import java.net.DatagramPacket;

/**
 * Abstract base class for all types of communication messages used by this RPC service.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
abstract class RPCMessage {

	/**
	 * The largest amount of data to be put in a single {@link DatagramPacket}.
	 */
	public static final int MAXIMUM_MSG_SIZE = 1018;

	/**
	 * The amount of data reserved for meta data in each {@link DatagramPacket}.
	 */
	public static final int METADATA_SIZE = 6;

	/**
	 * The message ID.
	 */
	private final int messageID;

	/**
	 * Constructs a new RPC message.
	 * 
	 * @param messageID
	 *        the message ID
	 */
	protected RPCMessage(final int messageID) {
		this.messageID = messageID;
	}

	/**
	 * Returns the message ID.
	 * 
	 * @return the message ID
	 */
	final int getMessageID() {
		return this.messageID;
	}
}
