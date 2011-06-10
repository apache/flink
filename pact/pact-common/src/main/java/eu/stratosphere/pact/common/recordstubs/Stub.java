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

package eu.stratosphere.pact.common.recordstubs;

import eu.stratosphere.nephele.configuration.Configuration;

/**
 * Abstract stub class for all PACT stubs. PACT stubs must be overwritten to
 * provide user implementations for PACT programs.
 * 
 * @author Fabian Hueske
 */
public abstract class Stub
{
	/**
	 * Initializes the key and value types of the stubs input and output.
	 */
	protected abstract void initTypes();

	/**
	 * Configures the stub. This method is called before the run() method is
	 * invoked. The method receives a Configuration object which holds
	 * parameters that were passed to the stub during plan construction. This
	 * method should be used to evaluate these parameters and configure the user
	 * stub implementation.
	 * 
	 * @see eu.stratosphere.nephele.configuration.Configuration
	 * @param parameters
	 */
	public abstract void configure(Configuration parameters);

	/**
	 * Initialization method for the stub. It is called after configure() and before the actual
	 * working methods (like <i>map</i> or <i>match</i>).
	 * This method should be used for initial setup of the stub implementation.
	 * <p>
	 * By default, this method does nothing.
	 * 
	 * @throws Exception Implementations may forward exceptions, which are caught by the runtime. When the
	 *                   runtime catches an exception, it aborts the task and lets the fail-over logic
	 *                   decide whether to retry the task execution.
	 */
	public void open() throws Exception
	{}

	/**
	 * Teardown method for the user code. It is called after the last call to the main working methods
	 * (e.g. <i>map</i> or <i>match</i>). It is called also when the task is aborted, in which case exceptions
	 * thrown by this method are logged but ignored. 
	 * <p>
	 * This method should be used for clean up.
	 * 
	 * @throws Exception Implementations may forward exceptions, which are caught by the runtime. When the
	 *                   runtime catches an exception, it aborts the task and lets the fail-over logic
	 *                   decide whether to retry the task execution.
	 */
	public void close() throws Exception
	{}
}
