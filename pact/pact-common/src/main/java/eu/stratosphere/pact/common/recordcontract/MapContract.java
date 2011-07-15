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

import eu.stratosphere.pact.common.recordstubs.MapStub;


/**
 * MapContract represents a Pact with a Map Input Contract.
 * InputContracts are second-order functions. They have one or multiple input sets of records and a first-order
 * user function (stub implementation).
 * <p> 
 * Map works on a single input and calls the first-order user function of a {@see eu.stratosphere.pact.common.stub.MapStub} 
 * for each record independently.
 * 
 * @see MapStub
 * 
 * @author Erik Nijkamp
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 */
public class MapContract extends SingleInputContract<MapStub>
{	
	private static String DEFAULT_NAME = "<Unnamed Mapper>";

	/**
	 * Creates a MapContract with the provided {@link MapStub} implementation
	 * and a default name.
	 * 
	 * @param c The {@link MapStub} implementation for this Map InputContract.
	 */
	public MapContract(Class<? extends MapStub> c) {
		this(c, DEFAULT_NAME);
	}
	
	/**
	 * Creates a MapContract with the provided {@link MapStub} implementation 
	 * and the given name. 
	 * 
	 * @param c The {@link MapStub} implementation for this Map InputContract.
	 * @param name The name of PACT.
	 */
	public MapContract(Class<? extends MapStub> c, String name) {
		super(c, name);
	}

	/**
	 * Creates a MapContract with the provided {@link MapStub} implementation the default name.
	 * It uses the given contract as its input.
	 * 
	 * @param c The {@link MapStub} implementation for this Map InputContract.
	 * @param input The contract to use as the input.
	 */
	public MapContract(Class<? extends MapStub> c, Contract input) {
		this(c, input, DEFAULT_NAME);
	}
	
	/**
	 * Creates a MapContract with the provided {@link MapStub} implementation and the given name.
	 * It uses the given contract as its input.
	 * 
	 * @param c The {@link MapStub} implementation for this Map InputContract.
	 * @param input The contract to use as the input.
	 * @param name The name of PACT.
	 */
	public MapContract(Class<? extends MapStub> c, Contract input, String name) {
		this(c, name);
		setInput(input);
	}
}
