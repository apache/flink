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
 * An exception specifying that a required key field was not (null) set in a record.
 */
public class NullKeyFieldException extends RuntimeException
{
	/**
	 * UID for serialization interoperability. 
	 */
	private static final long serialVersionUID = 5645812615711209578L;

	/**
     * Constructs an {@code NullKeyFieldException} with {@code null}
     * as its error detail message.
     */
	public NullKeyFieldException() {
		super();
	}

	/**
     * Constructs an {@code NullKeyFieldException} with the specified detail message.
     *
     * @param message The detail message.
     */
	public NullKeyFieldException(String message) {
		super(message);
	}
	
	/**
     * Constructs an {@code NullKeyFieldException} with a default message, referring to
     * given field number as the null key field.
     *
     * @param fieldNumber The index of the field that was null, bit expected to hold a key.
     */
	public NullKeyFieldException(int fieldNumber) {
		super("Field " + fieldNumber + " is null, but expected to hold a key.");
	}
}
