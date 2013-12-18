/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.taskmanager.transferenvelope;

import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferProvider;

/**
 * This exception is thrown to indicate that the deserialization process of a {@link TransferEnvelope} could not be
 * continued because a {@link Buffer} to store the envelope's content is currently not available.
 * 
 * @author warneke
 */
public final class NoBufferAvailableException extends Exception {

	/**
	 * Generated serial UID.
	 */
	private static final long serialVersionUID = -9164212953646457026L;

	/**
	 * The buffer provider which could not deliver a buffer.
	 */
	private final BufferProvider bufferProvider;

	/**
	 * Constructs a new exception.
	 * 
	 * @param bufferProvider
	 *        the buffer provider which could not deliver a buffer
	 */
	NoBufferAvailableException(final BufferProvider bufferProvider) {
		this.bufferProvider = bufferProvider;
	}

	/**
	 * Returns the buffer provider which could not deliver a buffer.
	 * 
	 * @return the buffer provider which could not deliver a buffer
	 */
	public BufferProvider getBufferProvider() {
		return this.bufferProvider;
	}
}
