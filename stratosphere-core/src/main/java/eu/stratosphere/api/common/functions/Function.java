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

package eu.stratosphere.api.common.functions;

import eu.stratosphere.configuration.Configuration;

/**
 * An base interface for all user-defined functions. This class defines methods for
 * the life cycle of the functions, as well as methods to access the context in which the functions
 * are executed.
 */
public interface Function {
	
	/**
	 * Initialization method for the function. It is called before the actual working methods 
	 * (like <i>map</i> or <i>join</i>) and thus suitable for one time setup work. For functions that
	 * are part of an iteration, this method will be invoked at the beginning of each iteration superstep.
	 * <p>
	 * The configuration object passed to the function can be used for configuration and initialization.
	 * The configuration contains all parameters that were configured on the function in the program
	 * composition.
	 * 
	 * <pre><blockquote>
	 * public class MyMapper extends FilterFunction<String> {
	 * 
	 *     private String searchString;
	 *     
	 *     public void open(Configuration parameters) {
	 *         this.searchString = parameters.getString("foo");
	 *     }
	 *     
	 *     public boolean filter(String value) {
	 *         return value.equals(searchString);
	 *     }
	 * }
	 * </blockquote></pre>
	 * <p>
	 * By default, this method does nothing.
	 * 
	 * @param parameters The configuration containing the parameters attached to the contract. 
	 * 
	 * @throws Exception Implementations may forward exceptions, which are caught by the runtime. When the
	 *                   runtime catches an exception, it aborts the task and lets the fail-over logic
	 *                   decide whether to retry the task execution.
	 * 
	 * @see eu.stratosphere.configuration.Configuration
	 */
	void open(Configuration parameters) throws Exception;

	/**
	 * Teardown method for the user code. It is called after the last call to the main working methods
	 * (e.g. <i>map</i> or <i>join</i>). For functions that  are part of an iteration, this method will
	 * be invoked after each iteration superstep.
	 * <p>
	 * This method can be used for clean up work.
	 * 
	 * @throws Exception Implementations may forward exceptions, which are caught by the runtime. When the
	 *                   runtime catches an exception, it aborts the task and lets the fail-over logic
	 *                   decide whether to retry the task execution.
	 */
	void close() throws Exception;
	
	
	/**
	 * Gets the context that contains information about the UDF's runtime.
	 * 
	 * Context information are for example {@link eu.stratosphere.api.common.accumulators.Accumulator}s
	 * or the {@link eu.stratosphere.api.common.cache.DistributedCache}.
	 * 
	 * @return The UDF's runtime context.
	 */
	RuntimeContext getRuntimeContext();
	
	/**
	 * Sets the function's runtime context. Called by the framework when creating a parallel instance of the function.
	 *  
	 * @param t The runtime context.
	 */
	void setRuntimeContext(RuntimeContext t);
}
