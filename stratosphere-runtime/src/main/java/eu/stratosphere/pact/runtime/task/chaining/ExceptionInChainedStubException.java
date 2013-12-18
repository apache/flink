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

package eu.stratosphere.pact.runtime.task.chaining;


/**
 * A special exception to indicate that an exception occurred in the nested call of a chained stub.
 * The exception's only purpose is to be  identifiable as such and to carry the cause exception.
 *
 * @author Stephan Ewen
 */
public class ExceptionInChainedStubException extends RuntimeException
{
	private static final long serialVersionUID = -7966910518892776903L;

	private String taskName;
	
	private Exception exception;
	
	/**
	 * Creates a new ExceptionInChainedStubException with the given cause.
	 * 
	 * @param cause The causing exception.
	 */
	public ExceptionInChainedStubException(String taskName, Exception wrappedException) {
		super();
		this.taskName = taskName;
		this.exception = wrappedException;
	}

	
	public String getTaskName() {
		return taskName;
	}

	
	public Exception getWrappedException() {
		return exception;
	}
}
