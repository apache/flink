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

package eu.stratosphere.pact.common.type;


/**
 * An exception specifying that the deserialization caused an error.
 */
public class DeserializationException extends RuntimeException
{
	/**
	 * UID for serialization interoperability. 
	 */
	private static final long serialVersionUID = -8725950711347033148L;

	/**
     * Constructs an {@code DeserializationException} with {@code null}
     * as its error detail message.
     */
	public DeserializationException() {
		super();
	}

	/**
     * Constructs an {@code DeserializationException} with the specified detail message
     * and cause.
     * 
     * @param message The detail message.
     * @param cause The cause of the exception.
     */
	public DeserializationException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
     * Constructs an {@code DeserializationException} with the specified detail message.
     *
     * @param message The detail message.
     */
	public DeserializationException(String message) {
		super(message);
	}

	/**
	 * Constructs an {@code DeserializationException} with the specified cause.
     * 
	 * @param cause The cause of the exception.
	 */
	public DeserializationException(Throwable cause) {
		super(cause);
	}
}
