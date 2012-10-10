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

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.generic.contract.Contract;
import eu.stratosphere.pact.generic.contract.GenericCoGroupContract;


/**
 * CrossContract represents a Match InputContract of the PACT Programming Model.
 * InputContracts are second-order functions. They have one or multiple input sets of records and a first-order
 * user function (stub implementation).
 * <p> 
 * CoGroup works on two inputs and calls the first-order user function of a {@link CoGroupStub} 
 * with the groups of records sharing the same key (one group per input) independently.
 * 
 * @see CoGroupStub
 */
public class CoGroupContract extends GenericCoGroupContract<CoGroupStub> implements RecordContract
{	
	private static String DEFAULT_NAME = "<Unnamed CoGrouper>";		// the default name for contracts
	
	/**
	 * The types of the keys that the contract operates on.
	 */
	private final Class<? extends Key>[] keyTypes;
	
	/**
	 * The ordering for the order inside a group from input one.
	 */
	private Ordering groupOrder1;
	
	/**
	 * The ordering for the order inside a group from input two.
	 */
	private Ordering groupOrder2;
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a Builder with the provided {@link CoGroupStub} implementation.
	 * 
	 * @param udf The {@link CoGroupStub} implementation for this CoGroup contract.
	 * @param keyClass The class of the key data type.
	 * @param keyColumn1 The position of the key in the first input's records.
	 * @param keyColumn2 The position of the key in the second input's records.
	 */
	public static Builder builder(Class<? extends CoGroupStub> udf, Class<? extends Key> keyClass,
			int keyColumn1, int keyColumn2) {
		return new Builder(udf, keyClass, keyColumn1, keyColumn2);
	}
	
	/**
	 * The private constructor that only gets invoked from the Builder.
	 * @param builder
	 */
	private CoGroupContract(Builder builder) {
		super(builder.udf, builder.getKeyColumnsArray1(), builder.getKeyColumnsArray2(), builder.name);
		this.keyTypes = builder.getKeyClassesArray();
		setFirstInputs(builder.inputs1);
		setSecondInputs(builder.inputs2);
		setGroupOrderForInputOne(builder.secondaryOrder1);
		setGroupOrderForInputTwo(builder.secondaryOrder2);
	}

	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.contract.RecordContract#getKeyClasses()
	 */
	@Override
	public Class<? extends Key>[] getKeyClasses() {
		return this.keyTypes;
	}
	
	/**
	 * Sets the order of the elements within a group for the given input.
	 * 
	 * @param inputNum The number of the input (here either <i>0</i> or <i>1</i>).
	 * @param order The order for the elements in a group.
	 */
	public void setGroupOrder(int inputNum, Ordering order) {
		if (inputNum == 0)
			this.groupOrder1 = order;
		else if (inputNum == 1)
			this.groupOrder2 = order;
		else
			throw new IndexOutOfBoundsException();
	}
	
	/**
	 * Sets the order of the elements within a group for the first input.
	 * 
	 * @param order The order for the elements in a group.
	 */
	public void setGroupOrderForInputOne(Ordering order) {
		setGroupOrder(0, order);
	}
	
	/**
	 * Sets the order of the elements within a group for the second input.
	 * 
	 * @param order The order for the elements in a group.
	 */
	public void setGroupOrderForInputTwo(Ordering order) {
		setGroupOrder(1, order);
	}
	
	/**
	 * Gets the value order for an input, i.e. the order of elements within a group.
	 * If no such order has been set, this method returns null.
	 * 
	 * @param inputNum The number of the input (here either <i>0</i> or <i>1</i>).
	 * @return The group order.
	 */
	public Ordering getGroupOrder(int inputNum) {
		if (inputNum == 0)
			return this.groupOrder1;
		else if (inputNum == 1)
			return this.groupOrder2;
		else
			throw new IndexOutOfBoundsException();
	}
	
	/**
	 * Gets the order of elements within a group for the first input.
	 * If no such order has been set, this method returns null.
	 * 
	 * @return The group order for the first input.
	 */
	public Ordering getGroupOrderForInputOne() {
		return getGroupOrder(0);
	}
	
	/**
	 * Gets the order of elements within a group for the second input.
	 * If no such order has been set, this method returns null.
	 * 
	 * @return The group order for the second input.
	 */
	public Ordering getGroupOrderForInputTwo() {
		return getGroupOrder(1);
	}

	/**
	 * Builder pattern, straight from Joshua Bloch's Effective Java (2nd Edition).
	 * 
	 * @author Aljoscha Krettek
	 */
	public static class Builder {
		
		/* The required parameters */
		private final Class<? extends CoGroupStub> udf;
		private final List<Class<? extends Key>> keyClasses;
		private final List<Integer> keyColumns1;
		private final List<Integer> keyColumns2;
		
		/* The optional parameters */
		private List<Contract> inputs1;
		private List<Contract> inputs2;
		private Ordering secondaryOrder1 = null;
		private Ordering secondaryOrder2 = null;
		private String name = DEFAULT_NAME;
		
		/**
		 * Creates a Builder with the provided {@link CoGroupStub} implementation.
		 * 
		 * @param udf The {@link CoGroupStub} implementation for this CoGroup contract.
		 * @param keyClass The class of the key data type.
		 * @param keyColumn1 The position of the key in the first input's records.
		 * @param keyColumn2 The position of the key in the second input's records.
		 */
		private Builder(Class<? extends CoGroupStub> udf, Class<? extends Key> keyClass,
				int keyColumn1, int keyColumn2)
		{
			this.udf = udf;
			this.keyClasses = new ArrayList<Class<? extends Key>>();
			this.keyClasses.add(keyClass);
			this.keyColumns1 = new ArrayList<Integer>();
			this.keyColumns1.add(keyColumn1);
			this.keyColumns2 = new ArrayList<Integer>();
			this.keyColumns2.add(keyColumn2);
			this.inputs1 = new ArrayList<Contract>();
			this.inputs2 = new ArrayList<Contract>();
		}
		
		private int[] getKeyColumnsArray1() {
			int[] result = new int[keyColumns1.size()];
			for (int i = 0; i < keyColumns1.size(); ++i) {
				result[i] = keyColumns1.get(i);
			}
			return result;
		}
		
		private int[] getKeyColumnsArray2() {
			int[] result = new int[keyColumns2.size()];
			for (int i = 0; i < keyColumns2.size(); ++i) {
				result[i] = keyColumns2.get(i);
			}
			return result;
		}
		
		@SuppressWarnings("unchecked")
		private Class<? extends Key>[] getKeyClassesArray() {
			return keyClasses.toArray(new Class[keyClasses.size()]);
		}

		/**
		 * Adds additional key field.
		 * 
		 * @param keyClass The class of the key data type.
		 * @param keyColumn1 The position of the key in the first input's records.
		 * @param keyColumn2 The position of the key in the second input's records.
		 */
		public Builder keyField(Class<? extends Key> keyClass, int keyColumn1, int keyColumn2) {
			keyClasses.add(keyClass);
			keyColumns1.add(keyColumn1);
			keyColumns2.add(keyColumn2);
			return this;
		}
		/**
		 * Sets the order of the elements within a group for the first input.
		 * 
		 * @param order The order for the elements in a group.
		 */
		public Builder secondaryOrder1(Ordering order) {
			this.secondaryOrder1 = order;
			return this;
		}
		
		/**
		 * Sets the order of the elements within a group for the second input.
		 * 
		 * @param order The order for the elements in a group.
		 */
		public Builder secondaryOrder2(Ordering order) {
			this.secondaryOrder2 = order;
			return this;
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
		 * Creates and returns a CoGroupContract from using the values given 
		 * to the builder.
		 * 
		 * @return The created contract
		 */
		public CoGroupContract build() {
			if (keyClasses.size() <= 0) {
				throw new IllegalStateException("At least one key attribute has to be set.");
			}
			return new CoGroupContract(this);
		}
	}
}
