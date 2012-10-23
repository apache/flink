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

package eu.stratosphere.pact.common.stubs;


/**
 * The execution context provides basic information about the parallel runtime context
 * in which a stub instance lives. Such information includes the current number of
 * parallel stub instances, the stub's parallel task index, the pact name, or the iteration context.
 *
 * @author Stephan Ewen
 */
public interface ExecutionContext
{
	/**
	 * Gets the name of the task. This is the name supplied to the contract upon instantiation. If
	 * no name was given at contract instantiation time, a default name will be returned.
	 * 
	 * @return The task's name.
	 */
	String getTaskName();
	
	/**
	 * Gets the number of parallel subtasks in which the stub is executed.
	 * 
	 * @return The number of parallel subtasks in which the stub is executed.
	 */
	int getNumberOfSubtasks();
	
	/**
	 * Gets the subtask's parallel task number.
	 * 
	 * @return The subtask's parallel task number.
	 */
	int getSubtaskIndex();
}
