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

package eu.stratosphere.types;


/**
 * An exception specifying that a required key field was not set in a record, i.e. was <code>null</code>.
 */
public class KeyFieldOutOfBoundsException extends RuntimeException
{
	/**
	 * UID for serialization interoperability. 
	 */
	private static final long serialVersionUID = 1538404143052384932L;
	
	private final int fieldNumber;

	/**
	 * Constructs an {@code KeyFieldOutOfBoundsException} with {@code null}
	 * as its error detail message.
	 */
	public KeyFieldOutOfBoundsException() {
		super();
		this.fieldNumber = -1;
	}

	/**
	 * Constructs an {@code KeyFieldOutOfBoundsException} with the specified detail message.
	 *
	 * @param message The detail message.
	 */
	public KeyFieldOutOfBoundsException(String message) {
		super(message);
		this.fieldNumber = -1;
	}
	
	/**
	 * Constructs an {@code KeyFieldOutOfBoundsException} with a default message, referring to
	 * given field number as the null key field.
	 *
	 * @param fieldNumber The index of the field that was null, bit expected to hold a key.
	 */
	public KeyFieldOutOfBoundsException(int fieldNumber) {
		super("Field " + fieldNumber + " is accessed for a key, but out of bounds in the record.");
		this.fieldNumber = fieldNumber;
	}
	
	/**
	 * Gets the field number that was attempted to access. If the number is not set, this method returns
	 * {@code -1}.
	 * 
	 * @return The field number that was attempted to access, or {@code -1}, if not set.
	 */
	public int getFieldNumber() {
		return this.fieldNumber;
	}
}
