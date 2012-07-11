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

import java.util.LinkedList;
import java.util.List;

import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.Key;


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
public class MatchContract extends DualInputContract<MatchStub>
{	
	private static String DEFAULT_NAME = "<Unnamed Matcher>";		// the default name for contracts
	
	// --------------------------------------------------------------------------------------------

	/**
	 * Creates a MatchContract with the provided {@link MatchStub} implementation
	 * and a default name. The match is performed on a single key column.
	 * 
	 * @param c The {@link MatchStub} implementation for this Match InputContract.
	 * @param keyType The class of the key data type.
	 * @param firstKeyColumn The position of the key in the first input's records.
	 * @param secondKeyColumn The position of the key in the second input's records.
	 */
	public MatchContract(Class<? extends MatchStub> c, Class<? extends Key> keyType, int firstKeyColumn, int secondKeyColumn) {
		this(c, keyType, firstKeyColumn, secondKeyColumn, DEFAULT_NAME);
	}
	
	/**
	 * Creates a MatchContract with the provided {@link MatchStub} implementation 
	 * and the given name. 
	 * 
	 * @param c The {@link MatchStub} implementation for this Match InputContract.
	 * @param keyType The class of the key data type.
	 * @param firstKeyColumn The position of the key in the first input's records.
	 * @param secondKeyColumn The position of the key in the second input's records.
	 * @param name The name of PACT.
	 */
	@SuppressWarnings("unchecked")
	public MatchContract(Class<? extends MatchStub> c, Class<? extends Key> keyType, int firstKeyColumn, int secondKeyColumn, String name) {
		this(c, new Class[] {keyType}, new int[] {firstKeyColumn}, new int[] {secondKeyColumn}, name);
	}
	
	/**
	 * Creates a MatchContract with the provided {@link MatchStub} implementation 
	 * and the given name. The match is performed on a single key column.
	 * 
	 * @param c The {@link MatchStub} implementation for this Match InputContract.
	 * @param keyType The classes of the key data types.
	 * @param firstKeyColumns The positions of the keys in the first input's records.
	 * @param secondKeyColumns The positions of the keys in the second input's records.
	 */
	public MatchContract(Class<? extends MatchStub> c, Class<? extends Key>[] keyTypes, int firstKeyColumns[], int secondKeyColumns[]) {
		this(c, keyTypes, firstKeyColumns, secondKeyColumns, DEFAULT_NAME);
	}
	
	/**
	 * Creates a MatchContract with the provided {@link MatchStub} implementation 
	 * and the given name. The match is performed on a single key column.
	 * 
	 * @param c The {@link MatchStub} implementation for this Match InputContract.
	 * @param keyTypes The classes of the key data types.
	 * @param firstKeyColumns The positions of the keys in the first input's records.
	 * @param secondKeyColumns The positions of the keys in the second input's records.
	 * @param name The name of PACT.
	 */
	public MatchContract(Class<? extends MatchStub> c, Class<? extends Key>[] keyTypes, int firstKeyColumns[], int secondKeyColumns[], String name) {
		super(c, keyTypes, firstKeyColumns, secondKeyColumns, name);
	}

	/**
	 * Creates a MatchContract with the provided {@link MatchStub} implementation the default name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link MatchStub} implementation for this Match InputContract.
	 * @param keyType The class of the key data type.
	 * @param firstKeyColumn The position of the key in the first input's records.
	 * @param secondKeyColumn The position of the key in the second input's records.
	 * @param input1 The contract to use as the first input.
	 * @param input2 The contract to use as the second input.
	 */
	public MatchContract(Class<? extends MatchStub> c, Class<? extends Key> keyType,
					int firstKeyColumn, int secondKeyColumn,
					Contract input1, Contract input2)
	{
		this(c, keyType, firstKeyColumn, secondKeyColumn, input1, input2, DEFAULT_NAME);
	}
	
	/**
	 * Creates a MatchContract with the provided {@link MatchStub} implementation the default name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link MatchStub} implementation for this Match InputContract.
	 * @param keyType The class of the key data type.
	 * @param firstKeyColumn The position of the key in the first input's records.
	 * @param secondKeyColumn The position of the key in the second input's records.
	 * @param input1 The contracts to use as the first input.
	 * @param input2 The contract to use as the second input.
	 */
	public MatchContract(Class<? extends MatchStub> c, Class<? extends Key> keyType,
					int firstKeyColumn, int secondKeyColumn,
					List<Contract> input1, Contract input2)
	{
		this(c, keyType, firstKeyColumn, secondKeyColumn, input1, input2, DEFAULT_NAME);
	}

	/**
	 * Creates a MatchContract with the provided {@link MatchStub} implementation the default name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link MatchStub} implementation for this Match InputContract.
	 * @param keyType The class of the key data type.
	 * @param firstKeyColumn The position of the key in the first input's records.
	 * @param secondKeyColumn The position of the key in the second input's records.
	 * @param input1 The contract to use as the first input.
	 * @param input2 The contracts to use as the second input.
	 */
	public MatchContract(Class<? extends MatchStub> c, Class<? extends Key> keyType,
					int firstKeyColumn, int secondKeyColumn,
					Contract input1, List<Contract> input2)
	{
		this(c, keyType, firstKeyColumn, secondKeyColumn, input1, input2, DEFAULT_NAME);
	}

	/**
	 * Creates a MatchContract with the provided {@link MatchStub} implementation the default name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link MatchStub} implementation for this Match InputContract.
	 * @param keyType The class of the key data type.
	 * @param firstKeyColumn The position of the key in the first input's records.
	 * @param secondKeyColumn The position of the key in the second input's records.
	 * @param input1 The contracts to use as the first input.
	 * @param input2 The contracts to use as the second input.
	 */
	public MatchContract(Class<? extends MatchStub> c, Class<? extends Key> keyType,
					int firstKeyColumn, int secondKeyColumn,
					List<Contract> input1, List<Contract> input2)
	{
		this(c, keyType, firstKeyColumn, secondKeyColumn, input1, input2, DEFAULT_NAME);
	}

	/**
	 * Creates a MatchContract with the provided {@link MatchStub} implementation and the given name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link MatchStub} implementation for this Match InputContract.
	 * @param keyType The class of the key data type.
	 * @param firstKeyColumn The position of the key in the first input's records.
	 * @param secondKeyColumn The position of the key in the second input's records.
	 * @param input1 The contract to use as the first input.
	 * @param input2 The contract to use as the second input.
	 * @param name The name of PACT.
	 */
	public MatchContract(Class<? extends MatchStub> c, Class<? extends Key> keyType, 
					int firstKeyColumn, int secondKeyColumn,
					Contract input1, Contract input2, String name)
	{
		this(c, keyType, firstKeyColumn, secondKeyColumn, name);
		setFirstInput(input1);
		setSecondInput(input2);
	}
	
	/**
	 * Creates a MatchContract with the provided {@link MatchStub} implementation and the given name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link MatchStub} implementation for this Match InputContract.
	 * @param keyType The class of the key data type.
	 * @param firstKeyColumn The position of the key in the first input's records.
	 * @param secondKeyColumn The position of the key in the second input's records.
	 * @param input1 The contracts to use as the first input.
	 * @param input2 The contract to use as the second input.
	 * @param name The name of PACT.
	 */
	public MatchContract(Class<? extends MatchStub> c, Class<? extends Key> keyType, 
					int firstKeyColumn, int secondKeyColumn,
					List<Contract> input1, Contract input2, String name)
	{
		this(c, keyType, firstKeyColumn, secondKeyColumn, name);
		setFirstInputs(input1);
		setSecondInput(input2);
	}

	/**
	 * Creates a MatchContract with the provided {@link MatchStub} implementation and the given name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link MatchStub} implementation for this Match InputContract.
	 * @param keyType The class of the key data type.
	 * @param firstKeyColumn The position of the key in the first input's records.
	 * @param secondKeyColumn The position of the key in the second input's records.
	 * @param input1 The contract to use as the first input.
	 * @param input2 The contracts to use as the second input.
	 * @param name The name of PACT.
	 */
	public MatchContract(Class<? extends MatchStub> c, Class<? extends Key> keyType, 
					int firstKeyColumn, int secondKeyColumn,
					Contract input1, List<Contract> input2, String name)
	{
		this(c, keyType, firstKeyColumn, secondKeyColumn, name);
		setFirstInput(input1);
		setSecondInputs(input2);
	}

	/**
	 * Creates a MatchContract with the provided {@link MatchStub} implementation and the given name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link MatchStub} implementation for this Match InputContract.
	 * @param keyType The class of the key data type.
	 * @param firstKeyColumn The position of the key in the first input's records.
	 * @param secondKeyColumn The position of the key in the second input's records.
	 * @param input1 The contracts to use as the first input.
	 * @param input2 The contracts to use as the second input.
	 * @param name The name of PACT.
	 */
	public MatchContract(Class<? extends MatchStub> c, Class<? extends Key> keyType, 
					int firstKeyColumn, int secondKeyColumn,
					List<Contract> input1, List<Contract> input2, String name)
	{
		this(c, keyType, firstKeyColumn, secondKeyColumn, name);
		setFirstInputs(input1);
		setSecondInputs(input2);
	}

	/**
	 * Creates a MatchContract with the provided {@link MatchStub} implementation the default name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link MatchStub} implementation for this Match InputContract.
	 * @param keyTypes The classes of the key data types.
	 * @param firstKeyColumns The positions of the keys in the first input's records.
	 * @param secondKeyColumns The positions of the keys in the second input's records.
	 * @param input1 The contract to use as the first input.
	 * @param input2 The contract to use as the second input.
	 */
	public MatchContract(Class<? extends MatchStub> c, 
					Class<? extends Key>[] keyTypes, int firstKeyColumns[], int secondKeyColumns[], 
					Contract input1, Contract input2)
	{
		this(c, keyTypes, firstKeyColumns, secondKeyColumns, input1, input2, DEFAULT_NAME);
	}
	
	/**
	 * Creates a MatchContract with the provided {@link MatchStub} implementation the default name.
	 * It uses the given contractys as its input.
	 * 
	 * @param c The {@link MatchStub} implementation for this Match InputContract.
	 * @param keyTypes The classes of the key data types.
	 * @param firstKeyColumns The positions of the keys in the first input's records.
	 * @param secondKeyColumns The positions of the keys in the second input's records.
	 * @param input1 The contracts to use as the first input.
	 * @param input2 The contract to use as the second input.
	 */
	public MatchContract(Class<? extends MatchStub> c, 
					Class<? extends Key>[] keyTypes, int firstKeyColumns[], int secondKeyColumns[], 
					List<Contract> input1, Contract input2)
	{
		this(c, keyTypes, firstKeyColumns, secondKeyColumns, input1, input2, DEFAULT_NAME);
	}

	/**
	 * Creates a MatchContract with the provided {@link MatchStub} implementation the default name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link MatchStub} implementation for this Match InputContract.
	 * @param keyTypes The classes of the key data types.
	 * @param firstKeyColumns The positions of the keys in the first input's records.
	 * @param secondKeyColumns The positions of the keys in the second input's records.
	 * @param input1 The contract to use as the first input.
	 * @param input2 The contracts to use as the second input.
	 */
	public MatchContract(Class<? extends MatchStub> c, 
					Class<? extends Key>[] keyTypes, int firstKeyColumns[], int secondKeyColumns[], 
					Contract input1, List<Contract> input2)
	{
		this(c, keyTypes, firstKeyColumns, secondKeyColumns, input1, input2, DEFAULT_NAME);
	}

	/**
	 * Creates a MatchContract with the provided {@link MatchStub} implementation the default name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link MatchStub} implementation for this Match InputContract.
	 * @param keyTypes The classes of the key data types.
	 * @param firstKeyColumns The positions of the keys in the first input's records.
	 * @param secondKeyColumns The positions of the keys in the second input's records.
	 * @param input1 The contracts to use as the first input.
	 * @param input2 The contracts to use as the second input.
	 */
	public MatchContract(Class<? extends MatchStub> c, 
					Class<? extends Key>[] keyTypes, int firstKeyColumns[], int secondKeyColumns[], 
					List<Contract> input1, List<Contract> input2)
	{
		this(c, keyTypes, firstKeyColumns, secondKeyColumns, input1, input2, DEFAULT_NAME);
	}

	/**
	 * Creates a MatchContract with the provided {@link MatchStub} implementation and the given name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link MatchStub} implementation for this Match InputContract.
	 * @param keyTypes The classes of the key data types.
	 * @param firstKeyColumns The positions of the keys in the first input's records.
	 * @param secondKeyColumns The positions of the keys in the second input's records.
	 * @param input1 The contract to use as the first input.
	 * @param input2 The contract to use as the second input.
	 * @param name The name of PACT.
	 */
	public MatchContract(Class<? extends MatchStub> c, Class<? extends Key>[] keyTypes, int firstKeyColumns[], int secondKeyColumns[], 
											Contract input1, Contract input2, String name)
	{
		this(c, keyTypes, firstKeyColumns, secondKeyColumns, name);
		setFirstInput(input1);
		setSecondInput(input2);
	}
	
	/**
	 * Creates a MatchContract with the provided {@link MatchStub} implementation and the given name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link MatchStub} implementation for this Match InputContract.
	 * @param keyTypes The classes of the key data types.
	 * @param firstKeyColumns The positions of the keys in the first input's records.
	 * @param secondKeyColumns The positions of the keys in the second input's records.
	 * @param input1 The contracts to use as the first input.
	 * @param input2 The contract to use as the second input.
	 * @param name The name of PACT.
	 */
	public MatchContract(Class<? extends MatchStub> c, Class<? extends Key>[] keyTypes, int firstKeyColumns[], int secondKeyColumns[], 
			List<Contract> input1, Contract input2, String name)
	{
		this(c, keyTypes, firstKeyColumns, secondKeyColumns, name);
		setFirstInputs(input1);
		setSecondInput(input2);
	}

	/**
	 * Creates a MatchContract with the provided {@link MatchStub} implementation and the given name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link MatchStub} implementation for this Match InputContract.
	 * @param keyTypes The classes of the key data types.
	 * @param firstKeyColumns The positions of the keys in the first input's records.
	 * @param secondKeyColumns The positions of the keys in the second input's records.
	 * @param input1 The contract to use as the first input.
	 * @param input2 The contracts to use as the second input.
	 * @param name The name of PACT.
	 */
	public MatchContract(Class<? extends MatchStub> c, Class<? extends Key>[] keyTypes, int firstKeyColumns[], int secondKeyColumns[], 
											Contract input1, List<Contract> input2, String name)
	{
		this(c, keyTypes, firstKeyColumns, secondKeyColumns, name);
		setFirstInput(input1);
		setSecondInputs(input2);
	}

	/**
	 * Creates a MatchContract with the provided {@link MatchStub} implementation and the given name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link MatchStub} implementation for this Match InputContract.
	 * @param keyTypes The classes of the key data types.
	 * @param firstKeyColumns The positions of the keys in the first input's records.
	 * @param secondKeyColumns The positions of the keys in the second input's records.
	 * @param input1 The contracts to use as the first input.
	 * @param input2 The contracts to use as the second input.
	 * @param name The name of PACT.
	 */
	public MatchContract(Class<? extends MatchStub> c, Class<? extends Key>[] keyTypes, int firstKeyColumns[], int secondKeyColumns[], 
			List<Contract> input1, List<Contract> input2, String name)
	{
		this(c, keyTypes, firstKeyColumns, secondKeyColumns, name);
		setFirstInputs(input1);
		setSecondInputs(input2);
	}
	
	/**
	 * The private constructor that only gets invoked from the Builder.
	 * @param builder
	 */
	private MatchContract(Builder builder) {
		super(builder.udf, builder.getKeyClassesArray(), builder.getKeyColumnsArray1(),
				builder.getKeyColumnsArray2(), builder.name);
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
		private final Class<? extends MatchStub> udf;
		private final List<Class<? extends Key>> keyClasses;
		private final List<Integer> keyColumns1;
		private final List<Integer> keyColumns2;
		
		/**
		 * The optional parameters.
		 */
		private List<Contract> inputs1;
		private List<Contract> inputs2;
		private String name = DEFAULT_NAME;
		
		/**
		 * Creates a Builder with the provided {@link CoGroupStub} implementation
		 * 
		 * @param udf The {@link CoGroupStub} implementation for this CoGroup InputContract.
		 * @param keyClass The class of the key data type.
		 * @param keyColumn1 The position of the key in the first input's records.
		 * @param keyColumn2 The position of the key in the second input's records.
		 */
		public Builder(Class<? extends MatchStub> udf, Class<? extends Key> keyClass, int keyColumn1, int keyColumn2) {
			this.udf = udf;
			this.keyClasses = new LinkedList<Class<? extends Key>>();
			this.keyClasses.add(keyClass);
			this.keyColumns1 = new LinkedList<Integer>();
			this.keyColumns1.add(keyColumn1);
			this.keyColumns2 = new LinkedList<Integer>();
			this.keyColumns2.add(keyColumn2);
			this.inputs1 = new LinkedList<Contract>();
			this.inputs2 = new LinkedList<Contract>();
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
		public Builder additionalKeyField(Class<? extends Key> keyClass, int keyColumn1, int keyColumn2) {
			keyClasses.add(keyClass);
			keyColumns1.add(keyColumn1);
			keyColumns2.add(keyColumn2);
			return this;
		}
		
		/**
		 * Sets the first input.
		 * 
		 * @param input
		 */
		public Builder input1(Contract input) {
			this.inputs1.clear();
			this.inputs1.add(input);
			return this;
		}
		
		/**
		 * Sets the first input.
		 * 
		 * @param input
		 */
		public Builder input2(Contract input) {
			this.inputs2.clear();
			this.inputs2.add(input);
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
		 * Creates and returns a MatchContract from using the values given 
		 * to the builder.
		 * 
		 * @return The created contract
		 */
		public MatchContract build() {
			return new MatchContract(this);
		}
	}
}
