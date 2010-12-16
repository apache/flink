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

package eu.stratosphere.nephele.execution;

/**
 * This exception is thrown if an error occurs that is related to the execution of tasks in Nephele.
 * 
 * @author warneke
 */

@Deprecated
public class ExecutionFailureException extends Exception {

	/**
	 * Generated serial version UID.
	 */
	private static final long serialVersionUID = -5204587294111452411L;

	/**
	 * The inner exception encapsulated by this exception type.
	 */
	private Exception innerException = null;

	/**
	 * Constructs an <code>ExecutionFailureException</code> with a specified error message.
	 * 
	 * @param msg
	 *        the error message to be transported through this exception.
	 */
	public ExecutionFailureException(String msg) {
		super(msg);
	}

	/**
	 * Constructs an <code>ExecutionFailureException</code> that encapsulates another exception.
	 * 
	 * @param innerException
	 *        the exception to be encapsulated
	 */
	public ExecutionFailureException(Exception innerException) {
		this.innerException = innerException;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {

		String msg = this.getClass().toString() + ": ";

		if (innerException != null) {
			msg += innerException.toString();
		} else {
			msg += super.getMessage();
		}

		return msg;
	}
}
