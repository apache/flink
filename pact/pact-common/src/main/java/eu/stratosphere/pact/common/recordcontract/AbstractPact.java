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

package eu.stratosphere.pact.common.recordcontract;

import eu.stratosphere.pact.common.recordstubs.Stub;


/**
 * Abstract superclass for all contracts that represent actual Pacts and no data sources or sinks.
 *
 * @author Stephan Ewen
 */
public abstract class AbstractPact<T extends Stub> extends Contract
{
	/**
	 * The class containing the user function for this Pact.
	 */
	protected Class<? extends T> stubClass;
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a new abstract Pact with the given name wrapping the given user function.
	 * 
	 * @param name The given name for the Pact, used in plans, logs and progress messages.
	 * @param stubClass The class containing the user function.
	 */
	protected AbstractPact(Class<? extends T> stubClass, String name)
	{
		super(name);
		this.stubClass = stubClass;
	}
	
	// --------------------------------------------------------------------------------------------

	/**
	 * Gets the stub that is wrapped by this contract. The stub is the actual implementation of the
	 * user code.
	 * 
	 * @return The class with the user function for this Pact.
	 */
	public Class<? extends T> getStubClass()
	{
		return this.stubClass;
	}
}
