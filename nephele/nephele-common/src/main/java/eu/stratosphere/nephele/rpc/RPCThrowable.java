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
 * This message is used to transport an exception of a remote procedure call back to the caller.
 * <p>
 * This message is thread-safe.
 * 
 * @author warneke
 */
final class RPCThrowable extends RPCResponse {

	/**
	 * The exception to be transported.
	 */
	private final Throwable throwable;

	/**
	 * Constructs a new RPC exception message.
	 * 
	 * @param messageID
	 *        the message ID
	 * @param throwable
	 *        the exception to be transported
	 */
	RPCThrowable(final int messageID, final Throwable throwable) {
		super(messageID);

		this.throwable = throwable;
	}

	/**
	 * The default constructor required by kryo.
	 */
	private RPCThrowable() {
		super(0);

		this.throwable = null;
	}

	/**
	 * Returns the transported exception.
	 * 
	 * @return the transported exception
	 */
	Throwable getThrowable() {
		return this.throwable;
	}
}
