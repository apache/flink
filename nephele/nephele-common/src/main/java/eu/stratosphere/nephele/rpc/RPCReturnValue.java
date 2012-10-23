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

package eu.stratosphere.nephele.rpc;

/**
 * This message is used to transport the return value of a remote procedure call back to the caller.
 * <p>
 * This message is thread-safe.
 * 
 * @author warneke
 */
final class RPCReturnValue extends RPCResponse {

	/**
	 * The return value of the remote procedure call.
	 */
	private final Object retVal;

	/**
	 * Constructs a new RPC return value message.
	 * 
	 * @param messageID
	 *        the message ID
	 * @param retVal
	 *        the return value of the remote procedure call
	 */
	RPCReturnValue(final int messageID, final Object retVal) {
		super(messageID);

		this.retVal = retVal;
	}

	/**
	 * The default constructor required by kryo.
	 */
	private RPCReturnValue() {
		super(0);

		this.retVal = null;
	}

	/**
	 * Returns the return value of the remote procedure call.
	 * 
	 * @return the return value of the remote procedure call
	 */
	Object getRetVal() {
		return this.retVal;
	}
}
