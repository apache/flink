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

package eu.stratosphere.pact.common.contract;

import java.util.List;

import eu.stratosphere.pact.common.stubs.CrossStub;


/**
 * CrossContract represents a Cross InputContract of the PACT Programming Model.
 *  InputContracts are second-order functions. They have one or multiple input sets of records and a first-order
 *  user function (stub implementation).
 * <p> 
 * Cross works on two inputs and calls the first-order function of a {@link CrossStub} 
 * for each combination of record from both inputs (each element of the Cartesian Product) independently.
 * 
 * @see CrossStub
 */
public class CrossContract extends DualInputContract<CrossStub>
{	
	private static String DEFAULT_NAME = "<Unnamed Crosser>";

	/**
	 * Creates a CrossContract with the provided {@link CrossStub} implementation
	 * and a default name.
	 * 
	 * @param c The {@link CrossStub} implementation for this Cross InputContract.
	 */
	public CrossContract(Class<? extends CrossStub> c) {
		this(c, DEFAULT_NAME);
	}
	
	/**
	 * Creates a CrossContract with the provided {@link CrossStub} implementation 
	 * and the given name. 
	 * 
	 * @param c The {@link CrossStub} implementation for this Cross InputContract.
	 * @param name The name of PACT.
	 */
	public CrossContract(Class<? extends CrossStub> c, String name) {
		super(c, name);
	}

	/**
	 * Creates a CrossContract with the provided {@link CrossStub} implementation the default name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link CrossStub} implementation for this Cross InputContract.
	 * @param input1 The contract to use as the first input.
	 * @param input2 The contract to use as the second input.
	 */
	public CrossContract(Class<? extends CrossStub> c, Contract input1, Contract input2) {
		this(c, input1, input2, DEFAULT_NAME);
	}
	
	/**
	 * Creates a CrossContract with the provided {@link CrossStub} implementation the default name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link CrossStub} implementation for this Cross InputContract.
	 * @param input1 The contracts to use as the first input.
	 * @param input2 The contract to use as the second input.
	 */
	public CrossContract(Class<? extends CrossStub> c, List<Contract> input1, Contract input2) {
		this(c, input1, input2, DEFAULT_NAME);
	}

	/**
	 * Creates a CrossContract with the provided {@link CrossStub} implementation the default name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link CrossStub} implementation for this Cross InputContract.
	 * @param input1 The contract to use as the first input.
	 * @param input2 The contracts to use as the second input.
	 */
	public CrossContract(Class<? extends CrossStub> c, Contract input1, List<Contract> input2) {
		this(c, input1, input2, DEFAULT_NAME);
	}

	/**
	 * Creates a CrossContract with the provided {@link CrossStub} implementation the default name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link CrossStub} implementation for this Cross InputContract.
	 * @param input1 The contracts to use as the first input.
	 * @param input2 The contracts to use as the second input.
	 */
	public CrossContract(Class<? extends CrossStub> c, List<Contract> input1, List<Contract> input2) {
		this(c, input1, input2, DEFAULT_NAME);
	}

	/**
	 * Creates a CrossContract with the provided {@link CrossStub} implementation and the given name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link CrossStub} implementation for this Cross InputContract.
	 * @param input1 The contract to use as the first input.
	 * @param input2 The contract to use as the second input.
	 * @param name The name of PACT.
	 */
	public CrossContract(Class<? extends CrossStub> c, Contract input1, Contract input2, String name) {
		this(c, name);
		setFirstInput(input1);
		setSecondInput(input2);
	}
	
	/**
	 * Creates a CrossContract with the provided {@link CrossStub} implementation and the given name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link CrossStub} implementation for this Cross InputContract.
	 * @param input1 The contracts to use as the first input.
	 * @param input2 The contract to use as the second input.
	 * @param name The name of PACT.
	 */
	public CrossContract(Class<? extends CrossStub> c, List<Contract> input1, Contract input2, String name) {
		this(c, name);
		setFirstInputs(input1);
		setSecondInput(input2);
	}

	/**
	 * Creates a CrossContract with the provided {@link CrossStub} implementation and the given name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link CrossStub} implementation for this Cross InputContract.
	 * @param input1 The contract to use as the first input.
	 * @param input2 The contracts to use as the second input.
	 * @param name The name of PACT.
	 */
	public CrossContract(Class<? extends CrossStub> c, Contract input1, List<Contract> input2, String name) {
		this(c, name);
		setFirstInput(input1);
		setSecondInputs(input2);
	}

	/**
	 * Creates a CrossContract with the provided {@link CrossStub} implementation and the given name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link CrossStub} implementation for this Cross InputContract.
	 * @param input1 The contracts to use as the first input.
	 * @param input2 The contracts to use as the second input.
	 * @param name The name of PACT.
	 */
	public CrossContract(Class<? extends CrossStub> c, List<Contract> input1, List<Contract> input2, String name) {
		this(c, name);
		setFirstInputs(input1);
		setSecondInputs(input2);
	}

}
