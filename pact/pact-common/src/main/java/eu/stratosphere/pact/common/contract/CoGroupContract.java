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

import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.type.Key;


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
public class CoGroupContract extends DualInputContract<CoGroupStub>
{	
	private static String DEFAULT_NAME = "<Unnamed CoGrouper>";		// the default name for contracts
	
	/**
	 * The classes that represent the secondary sort key data types.
	 */
	private Class<? extends Key>[] secondarySortKeyClasses;
	
	/**
	 * The positions of the keys in the tuples of the first input.
	 */
	private int[] secondarySortKeyFields1;
	
	/**
	 * The positions of the keys in the tuples of the second input.
	 */
	private int[] secondarySortKeyFields2;
	
	/**
	 * Creates a CoGroupContract with the provided {@link CoGroupStub} implementation
	 * and a default name. The match is performed on a single key column.
	 * 
	 * @param c The {@link CoGroupStub} implementation for this Match InputContract.
	 * @param keyType The class of the key data type.
	 * @param firstKeyColumn The position of the key in the first input's records.
	 * @param secondKeyColumn The position of the key in the second input's records.
	 */
	public CoGroupContract(Class<? extends CoGroupStub> c, Class<? extends Key> keyType, int firstKeyColumn, int secondKeyColumn) {
		this(c, keyType, firstKeyColumn, secondKeyColumn, DEFAULT_NAME);
	}
	
	/**
	 * Creates a CoGroupContract with the provided {@link CoGroupStub} implementation 
	 * and the given name. 
	 * 
	 * @param c The {@link CoGroupStub} implementation for this Match InputContract.
	 * @param keyType The class of the key data type.
	 * @param firstKeyColumn The position of the key in the first input's records.
	 * @param secondKeyColumn The position of the key in the second input's records.
	 * @param name The name of PACT.
	 */
	@SuppressWarnings("unchecked")
	public CoGroupContract(Class<? extends CoGroupStub> c, Class<? extends Key> keyType, int firstKeyColumn, int secondKeyColumn, String name) {
		this(c, new Class[] {keyType}, new int[] {firstKeyColumn}, new int[] {secondKeyColumn}, name);
	}
	
	/**
	 * Creates a CoGroupContract with the provided {@link CoGroupStub} implementation 
	 * and the given name. The match is performed on a single key column.
	 * 
	 * @param c The {@link CoGroupStub} implementation for this Match InputContract.
	 * @param keyType The classes of the key data types.
	 * @param firstKeyColumns The positions of the keys in the first input's records.
	 * @param secondKeyColumns The positions of the keys in the second input's records.
	 */
	public CoGroupContract(Class<? extends CoGroupStub> c, Class<? extends Key>[] keyTypes, int firstKeyColumns[], int secondKeyColumns[]) {
		this(c, keyTypes, firstKeyColumns, secondKeyColumns, DEFAULT_NAME);
	}
	
	/**
	 * Creates a CoGroupContract with the provided {@link CoGroupStub} implementation 
	 * and the given name. The match is performed on a single key column.
	 * 
	 * @param c The {@link CoGroupStub} implementation for this Match InputContract.
	 * @param keyTypes The classes of the key data types.
	 * @param firstKeyColumns The positions of the keys in the first input's records.
	 * @param secondKeyColumns The positions of the keys in the second input's records.
	 * @param name The name of PACT.
	 */
	@SuppressWarnings("unchecked")
	public CoGroupContract(Class<? extends CoGroupStub> c, Class<? extends Key>[] keyTypes, int firstKeyColumns[], int secondKeyColumns[], String name) {
		super(c, keyTypes, firstKeyColumns, secondKeyColumns, name);
		this.secondarySortKeyClasses = (Class<? extends Key>[]) new Class[0];
		this.secondarySortKeyFields1 = this.secondarySortKeyFields2 = new int[0];
	}

	/**
	 * Creates a CoGroupContract with the provided {@link CoGroupStub} implementation the default name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link CoGroupStub} implementation for this Match InputContract.
	 * @param keyType The class of the key data type.
	 * @param firstKeyColumn The position of the key in the first input's records.
	 * @param secondKeyColumn The position of the key in the second input's records.
	 * @param input1 The contract to use as the first input.
	 * @param input2 The contract to use as the second input.
	 */
	public CoGroupContract(Class<? extends CoGroupStub> c, Class<? extends Key> keyType,
					int firstKeyColumn, int secondKeyColumn,
					Contract input1, Contract input2)
	{
		this(c, keyType, firstKeyColumn, secondKeyColumn, input1, input2, DEFAULT_NAME);
	}
	
	/**
	 * Creates a CoGroupContract with the provided {@link CoGroupStub} implementation the default name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link CoGroupStub} implementation for this Match InputContract.
	 * @param keyType The class of the key data type.
	 * @param firstKeyColumn The position of the key in the first input's records.
	 * @param secondKeyColumn The position of the key in the second input's records.
	 * @param input1 The contracts to use as the first input.
	 * @param input2 The contract to use as the second input.
	 */
	public CoGroupContract(Class<? extends CoGroupStub> c, Class<? extends Key> keyType,
					int firstKeyColumn, int secondKeyColumn,
					List<Contract> input1, Contract input2)
	{
		this(c, keyType, firstKeyColumn, secondKeyColumn, input1, input2, DEFAULT_NAME);
	}

	/**
	 * Creates a CoGroupContract with the provided {@link CoGroupStub} implementation the default name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link CoGroupStub} implementation for this Match InputContract.
	 * @param keyType The class of the key data type.
	 * @param firstKeyColumn The position of the key in the first input's records.
	 * @param secondKeyColumn The position of the key in the second input's records.
	 * @param input1 The contract to use as the first input.
	 * @param input2 The contracts to use as the second input.
	 */
	public CoGroupContract(Class<? extends CoGroupStub> c, Class<? extends Key> keyType,
					int firstKeyColumn, int secondKeyColumn,
					Contract input1, List<Contract> input2)
	{
		this(c, keyType, firstKeyColumn, secondKeyColumn, input1, input2, DEFAULT_NAME);
	}

	/**
	 * Creates a CoGroupContract with the provided {@link CoGroupStub} implementation the default name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link CoGroupStub} implementation for this Match InputContract.
	 * @param keyType The class of the key data type.
	 * @param firstKeyColumn The position of the key in the first input's records.
	 * @param secondKeyColumn The position of the key in the second input's records.
	 * @param input1 The contracts to use as the first input.
	 * @param input2 The contracts to use as the second input.
	 */
	public CoGroupContract(Class<? extends CoGroupStub> c, Class<? extends Key> keyType,
					int firstKeyColumn, int secondKeyColumn,
					List<Contract> input1, List<Contract> input2)
	{
		this(c, keyType, firstKeyColumn, secondKeyColumn, input1, input2, DEFAULT_NAME);
	}

	/**
	 * Creates a CoGroupContract with the provided {@link CoGroupStub} implementation and the given name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link CoGroupStub} implementation for this Match InputContract.
	 * @param keyType The class of the key data type.
	 * @param firstKeyColumn The position of the key in the first input's records.
	 * @param secondKeyColumn The position of the key in the second input's records.
	 * @param input1 The contract to use as the first input.
	 * @param input2 The contract to use as the second input.
	 * @param name The name of PACT.
	 */
	public CoGroupContract(Class<? extends CoGroupStub> c, Class<? extends Key> keyType, 
					int firstKeyColumn, int secondKeyColumn,
					Contract input1, Contract input2, String name)
	{
		this(c, keyType, firstKeyColumn, secondKeyColumn, name);
		setFirstInput(input1);
		setSecondInput(input2);
	}
	
	/**
	 * Creates a CoGroupContract with the provided {@link CoGroupStub} implementation and the given name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link CoGroupStub} implementation for this Match InputContract.
	 * @param keyType The class of the key data type.
	 * @param firstKeyColumn The position of the key in the first input's records.
	 * @param secondKeyColumn The position of the key in the second input's records.
	 * @param input1 The contracts to use as the first input.
	 * @param input2 The contract to use as the second input.
	 * @param name The name of PACT.
	 */
	public CoGroupContract(Class<? extends CoGroupStub> c, Class<? extends Key> keyType, 
					int firstKeyColumn, int secondKeyColumn,
					List<Contract> input1, Contract input2, String name)
	{
		this(c, keyType, firstKeyColumn, secondKeyColumn, name);
		setFirstInputs(input1);
		setSecondInput(input2);
	}

	/**
	 * Creates a CoGroupContract with the provided {@link CoGroupStub} implementation and the given name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link CoGroupStub} implementation for this Match InputContract.
	 * @param keyType The class of the key data type.
	 * @param firstKeyColumn The position of the key in the first input's records.
	 * @param secondKeyColumn The position of the key in the second input's records.
	 * @param input1 The contract to use as the first input.
	 * @param input2 The contracts to use as the second input.
	 * @param name The name of PACT.
	 */
	public CoGroupContract(Class<? extends CoGroupStub> c, Class<? extends Key> keyType, 
					int firstKeyColumn, int secondKeyColumn,
					Contract input1, List<Contract> input2, String name)
	{
		this(c, keyType, firstKeyColumn, secondKeyColumn, name);
		setFirstInput(input1);
		setSecondInputs(input2);
	}

	/**
	 * Creates a CoGroupContract with the provided {@link CoGroupStub} implementation and the given name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link CoGroupStub} implementation for this Match InputContract.
	 * @param keyType The class of the key data type.
	 * @param firstKeyColumn The position of the key in the first input's records.
	 * @param secondKeyColumn The position of the key in the second input's records.
	 * @param input1 The contracts to use as the first input.
	 * @param input2 The contracts to use as the second input.
	 * @param name The name of PACT.
	 */
	public CoGroupContract(Class<? extends CoGroupStub> c, Class<? extends Key> keyType, 
					int firstKeyColumn, int secondKeyColumn,
					List<Contract> input1, List<Contract> input2, String name)
	{
		this(c, keyType, firstKeyColumn, secondKeyColumn, name);
		setFirstInputs(input1);
		setSecondInputs(input2);
	}

	/**
	 * Creates a CoGroupContract with the provided {@link CoGroupStub} implementation the default name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link CoGroupStub} implementation for this Match InputContract.
	 * @param keyTypes The classes of the key data types.
	 * @param firstKeyColumns The positions of the keys in the first input's records.
	 * @param secondKeyColumns The positions of the keys in the second input's records.
	 * @param input1 The contract to use as the first input.
	 * @param input2 The contract to use as the second input.
	 */
	public CoGroupContract(Class<? extends CoGroupStub> c, 
					Class<? extends Key>[] keyTypes, int firstKeyColumns[], int secondKeyColumns[], 
					Contract input1, Contract input2)
	{
		this(c, keyTypes, firstKeyColumns, secondKeyColumns, input1, input2, DEFAULT_NAME);
	}
	
	/**
	 * Creates a CoGroupContract with the provided {@link CoGroupStub} implementation the default name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link CoGroupStub} implementation for this Match InputContract.
	 * @param keyTypes The classes of the key data types.
	 * @param firstKeyColumns The positions of the keys in the first input's records.
	 * @param secondKeyColumns The positions of the keys in the second input's records.
	 * @param input1 The contracts to use as the first input.
	 * @param input2 The contract to use as the second input.
	 */
	public CoGroupContract(Class<? extends CoGroupStub> c, 
					Class<? extends Key>[] keyTypes, int firstKeyColumns[], int secondKeyColumns[], 
					List<Contract> input1, Contract input2)
	{
		this(c, keyTypes, firstKeyColumns, secondKeyColumns, input1, input2, DEFAULT_NAME);
	}

	/**
	 * Creates a CoGroupContract with the provided {@link CoGroupStub} implementation the default name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link CoGroupStub} implementation for this Match InputContract.
	 * @param keyTypes The classes of the key data types.
	 * @param firstKeyColumns The positions of the keys in the first input's records.
	 * @param secondKeyColumns The positions of the keys in the second input's records.
	 * @param input1 The contract to use as the first input.
	 * @param input2 The contracts to use as the second input.
	 */
	public CoGroupContract(Class<? extends CoGroupStub> c, 
					Class<? extends Key>[] keyTypes, int firstKeyColumns[], int secondKeyColumns[], 
					Contract input1, List<Contract> input2)
	{
		this(c, keyTypes, firstKeyColumns, secondKeyColumns, input1, input2, DEFAULT_NAME);
	}

	/**
	 * Creates a CoGroupContract with the provided {@link CoGroupStub} implementation the default name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link CoGroupStub} implementation for this Match InputContract.
	 * @param keyTypes The classes of the key data types.
	 * @param firstKeyColumns The positions of the keys in the first input's records.
	 * @param secondKeyColumns The positions of the keys in the second input's records.
	 * @param input1 The contracts to use as the first input.
	 * @param input2 The contracts to use as the second input.
	 */
	public CoGroupContract(Class<? extends CoGroupStub> c, 
					Class<? extends Key>[] keyTypes, int firstKeyColumns[], int secondKeyColumns[], 
					List<Contract> input1, List<Contract> input2)
	{
		this(c, keyTypes, firstKeyColumns, secondKeyColumns, input1, input2, DEFAULT_NAME);
	}

	/**
	 * Creates a CoGroupContract with the provided {@link CoGroupStub} implementation and the given name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link CoGroupStub} implementation for this Match InputContract.
	 * @param keyTypes The classes of the key data types.
	 * @param firstKeyColumns The positions of the keys in the first input's records.
	 * @param secondKeyColumns The positions of the keys in the second input's records.
	 * @param input1 The contract to use as the first input.
	 * @param input2 The contract to use as the second input.
	 * @param name The name of PACT.
	 */
	public CoGroupContract(Class<? extends CoGroupStub> c, Class<? extends Key>[] keyTypes, int firstKeyColumns[], int secondKeyColumns[], 
											Contract input1, Contract input2, String name)
	{
		this(c, keyTypes, firstKeyColumns, secondKeyColumns, name);
		setFirstInput(input1);
		setSecondInput(input2);
	}
	
	/**
	 * Creates a CoGroupContract with the provided {@link CoGroupStub} implementation and the given name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link CoGroupStub} implementation for this Match InputContract.
	 * @param keyTypes The classes of the key data types.
	 * @param firstKeyColumns The positions of the keys in the first input's records.
	 * @param secondKeyColumns The positions of the keys in the second input's records.
	 * @param input1 The contracts to use as the first input.
	 * @param input2 The contract to use as the second input.
	 * @param name The name of PACT.
	 */
	public CoGroupContract(Class<? extends CoGroupStub> c, Class<? extends Key>[] keyTypes, int firstKeyColumns[], int secondKeyColumns[], 
			List<Contract> input1, Contract input2, String name)
	{
		this(c, keyTypes, firstKeyColumns, secondKeyColumns, name);
		setFirstInputs(input1);
		setSecondInput(input2);
	}

	/**
	 * Creates a CoGroupContract with the provided {@link CoGroupStub} implementation and the given name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link CoGroupStub} implementation for this Match InputContract.
	 * @param keyTypes The classes of the key data types.
	 * @param firstKeyColumns The positions of the keys in the first input's records.
	 * @param secondKeyColumns The positions of the keys in the second input's records.
	 * @param input1 The contract to use as the first input.
	 * @param input2 The contracts to use as the second input.
	 * @param name The name of PACT.
	 */
	public CoGroupContract(Class<? extends CoGroupStub> c, Class<? extends Key>[] keyTypes, int firstKeyColumns[], int secondKeyColumns[], 
			Contract input1, List<Contract> input2, String name)
	{
		this(c, keyTypes, firstKeyColumns, secondKeyColumns, name);
		setFirstInput(input1);
		setSecondInputs(input2);
	}

	/**
	 * Creates a CoGroupContract with the provided {@link CoGroupStub} implementation and the given name.
	 * It uses the given contracts as its input.
	 * 
	 * @param c The {@link CoGroupStub} implementation for this Match InputContract.
	 * @param keyTypes The classes of the key data types.
	 * @param firstKeyColumns The positions of the keys in the first input's records.
	 * @param secondKeyColumns The positions of the keys in the second input's records.
	 * @param input1 The contracts to use as the first input.
	 * @param input2 The contracts to use as the second input.
	 * @param name The name of PACT.
	 */
	public CoGroupContract(Class<? extends CoGroupStub> c, Class<? extends Key>[] keyTypes, int firstKeyColumns[], int secondKeyColumns[], 
			List<Contract> input1, List<Contract> input2, String name)
	{
		this(c, keyTypes, firstKeyColumns, secondKeyColumns, name);
		setFirstInputs(input1);
		setSecondInputs(input2);
	}

	/**
	 * Gets the types of the secondary sort key fields on which this contract groups.
	 * 
	 * @return The types of the key fields.
	 */
	public void setSecondarySortKeyClasses(Class<? extends Key>[] keys)
	{
		this.secondarySortKeyClasses = keys;
	}
	
	/**
	 * Gets the types of the secondary sort key fields on which this contract groups.
	 * 
	 * @return The types of the key fields.
	 */
	public Class<? extends Key>[] getSecondarySortKeyClasses()
	{
		return this.secondarySortKeyClasses;
	}
	
	/**
	 * Sets the column numbers of the key fields in the input records for the given input.
	 */
	public void setSecondarySortKeyColumnNumbers(int inputNum, int[] positions) {
		if (inputNum == 0) {
			this.secondarySortKeyFields1 = positions;
		}
		else if (inputNum == 1) {
			this.secondarySortKeyFields2 = positions;
		}
		else throw new IndexOutOfBoundsException();
	}
	
	/**
	 * Gets the column numbers of the secondary sort key fields in the input records for the given input.
	 *  
	 * @return The column numbers of the key fields.
	 */
	public int[] getSecondarySortKeyColumnNumbers(int inputNum) {
		if (inputNum == 0) {
			return this.secondarySortKeyFields1;
		}
		else if (inputNum == 1) {
			return this.secondarySortKeyFields2;
		}
		else throw new IndexOutOfBoundsException();
	}
}
