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
	
	/**
	 * The private constructor that only gets invoked from the Builder.
	 * @param builder
	 */
	private CrossContract(Builder builder) {
		super(builder.udf, builder.name);
		setFirstInputs(builder.inputs1);
		setSecondInputs(builder.inputs2);
	}

	/**
	 * Builder pattern, straight from Joshua Bloch's Effective Java (2nd Edition).
	 * 
	 * @author Aljoscha Krettek
	 */
	public static class Builder {
		/**
		 * The required parameters.
		 */
		private final Class<? extends CrossStub> udf;
		
		/**
		 * The optional parameters.
		 */
		private List<Contract> inputs1;
		private List<Contract> inputs2;
		private String name = DEFAULT_NAME;
		
		/**
		 * Creates a Builder with the provided {@link CoGroupStub} implementation.
		 * 
		 * @param udf The {@link CoGroupStub} implementation for this CoGroup InputContract.
		 */
		public Builder(Class<? extends CrossStub> udf) {
			this.udf = udf;
		}
		
		/**
		 * Sets one or several inputs (union) for input 1.
		 * 
		 * @param input
		 */
		public Builder input1(Contract ...inputs) {
			this.inputs1.clear();
			for (Contract c : inputs) {
				this.inputs1.add(c);
			}
			return this;
		}
		
		/**
		 * Sets one or several inputs (union) for input 2.
		 * 
		 * @param input
		 */
		public Builder input2(Contract ...inputs) {
			this.inputs2.clear();
			for (Contract c : inputs) {
				this.inputs2.add(c);
			}
			return this;
		}
		
		/**
		 * Sets the first inputs.
		 * 
		 * @param inputs
		 */
		public Builder inputs1(List<Contract> inputs) {
			this.inputs1 = inputs;
			return this;
		}
		
		/**
		 * Sets the second inputs.
		 * 
		 * @param inputs
		 */
		public Builder inputs2(List<Contract> inputs) {
			this.inputs2 = inputs;
			return this;
		}
		
		/**
		 * Sets the name of this contract.
		 * 
		 * @param name
		 */
		public Builder name(String name) {
			this.name = name;
			return this;
		}
		
		/**
		 * Creates and returns a CrossContract from using the values given 
		 * to the builder.
		 * 
		 * @return The created contract
		 */
		public CrossContract build() {
			return new CrossContract(this);
		}
	}
}
