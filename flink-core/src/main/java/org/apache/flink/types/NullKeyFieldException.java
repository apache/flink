/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.types;


/**
 * An exception specifying that a required key field was not set in a record, i.e. was <code>null</code>.
 */
public class NullKeyFieldException extends RuntimeException
{
	/**
	 * UID for serialization interoperability. 
	 */
	private static final long serialVersionUID = -3254501285363420762L;
	
	private final int fieldNumber;

	/**
	 * Constructs an {@code NullKeyFieldException} with {@code null}
	 * as its error detail message.
	 */
	public NullKeyFieldException() {
		super();
		this.fieldNumber = -1;
	}

	/**
	 * Constructs an {@code NullKeyFieldException} with a default message, referring to
	 * the field number given in the {@code NullFieldException}.
	 *
	 * @param nfex The base exception.
	 */
	public NullKeyFieldException(NullFieldException nfex) {
		super();
		this.fieldNumber = nfex.getFieldPos();
	}
	
	/**
	 * Constructs an {@code NullKeyFieldException} with the specified detail message.
	 *
	 * @param message The detail message.
	 */
	public NullKeyFieldException(String message) {
		super(message);
		this.fieldNumber = -1;
	}
	
	/**
	 * Constructs an {@code NullKeyFieldException} with a default message, referring to
	 * given field number as the null key field.
	 *
	 * @param fieldNumber The index of the field that was null, bit expected to hold a key.
	 */
	public NullKeyFieldException(int fieldNumber) {
		super("Field " + fieldNumber + " is null, but expected to hold a key.");
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
