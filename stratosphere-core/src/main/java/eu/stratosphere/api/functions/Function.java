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

package eu.stratosphere.api.functions;

import eu.stratosphere.configuration.Configuration;

/**
 * Abstract stub class for all PACT stubs. PACT stubs must be overwritten to provide user implementations
 * for PACT programs.
 */
public interface Function {
	
	/**
	 * Initialization method for the stub. It is called before the actual working methods 
	 * (like <i>map</i> or <i>match</i>). This method should be used for configuration and 
	 * initialization of the stub implementation.
	 * <p>
	 * This method receives the parameters attached to the contract. Consider the following pseudo code example,
	 * which realizes a parameterizable filter:
	 * <code>
	 * public Plan getPlan(String... args)
	 * {
	 *     MapContract mc = new MapContract(MyMapper.class, "My Mapper");
	 *     mc.setDegreeOfParallelism(48);
	 *     mc.getStubParameters().setString("foo", "bar");
	 *     
	 *      ...
	 *      
	 *      Plan plan = new Plan(...);
	 *      return plan;
	 * }
	 * 
	 * public class MyMapper extends MapStub {
	 * 
	 *     private String searchString;
	 *     
	 *     public void open(Configuration parameters) {
	 *         this.searchString = parameters.getString("foo", null);
	 *         // searchString will be "bar" when job is started
	 *     }
	 *     
	 *     public void map(PactRecord record, Collector collector) {
	 *         if ( record.getValue(0, PactString.class).equals(this.searchString) ) {
	 *             collector.emit(record);
	 *         }
	 *     }
	 * }
	 * </code>
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
	 * (e.g. <i>map</i> or <i>match</i>). It is called also when the task is aborted, in which case exceptions
	 * thrown by this method are logged but ignored. 
	 * <p>
	 * This method should be used for clean up.
	 * 
	 * @throws Exception Implementations may forward exceptions, which are caught by the runtime. When the
	 *                   runtime catches an exception, it aborts the task and lets the fail-over logic
	 *                   decide whether to retry the task execution.
	 */
	void close() throws Exception;
	
	
	/**
	 * Gets the context that contains information about the UDF's runtime.
	 * 
	 * @return The UDF's runtime context.
	 */
	RuntimeContext getRuntimeContext();
	
	/**
	 * Sets the stub's runtime context. Called by the framework when creating a parallel instance of the stub.
	 *  
	 * @param t The runtime context.
	 */
	void setRuntimeContext(RuntimeContext t);
}
