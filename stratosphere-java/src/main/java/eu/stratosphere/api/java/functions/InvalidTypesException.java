/***********************************************************************************************************************
 *
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
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.java.functions;

import eu.stratosphere.api.common.InvalidProgramException;

/**
 * A special case of the {@link InvalidProgramException}, indicating that the types used in
 * an operation are invalid or inconsistent. 
 */
public class InvalidTypesException extends InvalidProgramException {

	private static final long serialVersionUID = 1L;

	/**
	 * Creates a new exception with no message.
	 */
	public InvalidTypesException() {
		super();
	}

	/**
	 * Creates a new exception with the given message.
	 * 
	 * @param message The exception message.
	 */
	public InvalidTypesException(String message) {
		super(message);
	}
}
