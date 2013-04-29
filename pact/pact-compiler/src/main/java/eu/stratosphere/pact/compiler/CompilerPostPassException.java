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

package eu.stratosphere.pact.compiler;

/**
 * An exception that is thrown by the pact compiler when encountering
 * a problem during the optimizer post pass. This is a dedicated exception
 * because it is thrown by user-specified optimizer extensions.
 */
public class CompilerPostPassException extends CompilerException
{
	/**
	 * Serial version UID for serialization interoperability.
	 */
	private static final long serialVersionUID = -322650826288034623L;

	/**
	 * Creates a post pass exception with no message and no cause.
	 */
	public CompilerPostPassException() {}

	/**
	 * Creates a post pass exception with the given message and no cause.
	 * 
	 * @param message The message for the exception.
	 */
	public CompilerPostPassException(String message) {
		super(message);
	}

	/**
	 * Creates a post pass exception with the given cause and no message.
	 * 
	 * @param cause The <tt>Throwable</tt> that caused this exception.
	 */
	public CompilerPostPassException(Throwable cause) {
		super(cause);
	}

	/**
	 * Creates a post pass exception with the given message and cause.
	 * 
	 * @param message The message for the exception.
	 * @param cause The <tt>Throwable</tt> that caused this exception.
	 */
	public CompilerPostPassException(String message, Throwable cause) {
		super(message, cause);
	}
}
