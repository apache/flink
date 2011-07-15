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

import eu.stratosphere.pact.common.recordstubs.MatchStub;


/**
 * CrossContract represents a Match InputContract of the PACT Programming Model.
 * InputContracts are second-order functions. They have one or multiple input sets of records and a first-order
 * user function (stub implementation).
 * <p> 
 * Match works on two inputs and calls the first-order function of a {@link MatchStub} 
 * for each combination of record from both inputs that share the same key independently. In that sense, it is very
 * similar to an inner join.
 * 
 * @see MatchStub
 */
public class MatchContract extends DualInputContract<MatchStub<?>>
{	
	private static String DEFAULT_NAME = "<Unnamed Matcher>";

	/**
	 * Creates a MatchContract with the provided {@link MatchStub} implementation
	 * and a default name.
	 * 
	 * @param c The {@link MatchStub} implementation for this Match InputContract.
	 */
	public MatchContract(Class<? extends MatchStub<?>> c) {
		this(c, DEFAULT_NAME);
	}
	
	/**
	 * Creates a MatchContract with the provided {@link MatchStub} implementation 
	 * and the given name. 
	 * 
	 * @param c The {@link MatchStub} implementation for this Match InputContract.
	 * @param name The name of PACT.
	 */
	public MatchContract(Class<? extends MatchStub<?>> c, String name) {
		super(c, name);
	}

	/**
	 * Creates a MatchContract with the provided {@link MatchStub} implementation the default name.
	 * It uses the given contract as its input.
	 * 
	 * @param c The {@link MatchStub} implementation for this Match InputContract.
	 * @param input1 The contract to use as the first input.
	 * @param input2 The contract to use as the second input.
	 */
	public MatchContract(Class<? extends MatchStub<?>> c, Contract input1, Contract input2) {
		this(c, input1, input2, DEFAULT_NAME);
	}
	
	/**
	 * Creates a MatchContract with the provided {@link MatchStub} implementation and the given name.
	 * It uses the given contract as its input.
	 * 
	 * @param c The {@link MatchStub} implementation for this Match InputContract.
	 * @param input1 The contract to use as the first input.
	 * @param input2 The contract to use as the second input.
	 * @param name The name of PACT.
	 */
	public MatchContract(Class<? extends MatchStub<?>> c, Contract input1, Contract input2, String name) {
		this(c, name);
		setFirstInput(input1);
		setSecondInput(input2);
	}
}
