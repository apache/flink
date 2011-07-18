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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import eu.stratosphere.pact.common.recordstubs.ReduceStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.util.ReflectionUtil;


/**
 * MapContract represents a Pact with a Map Input Contract.
 * InputContracts are second-order functions. They have one or multiple input sets of records and a first-order
 * user function (stub implementation).
 * <p> 
 * Map works on a single input and calls the first-order user function of a {@see eu.stratosphere.pact.common.stub.MapStub} 
 * for each record independently.
 * 
 * @see ReduceStub
 */
public class ReduceContract extends SingleInputContract<ReduceStub<?>>
{	
	private static final String DEFAULT_NAME = "<Unnamed Reducer>";	// the default name for contracts
	
	private Class<? extends Key> keyClass;							// the class of the key
	
	private final int[] keyFields;									// the positions of the key fields in the record

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a ReduceContract with the provided {@link ReduceStub} implementation
	 * and a default name.
	 * 
	 * @param c The {@link ReduceStub} implementation for this Reduce InputContract.
	 * @param keyColumn The position of the key in the input records.
	 */
	public ReduceContract(Class<? extends ReduceStub<?>> c, int keyColumn) {
		this(c, new int[] {keyColumn} );
	}
	
	/**
	 * Creates a ReduceContract with the provided {@link ReduceStub} implementation
	 * and a default name. The reducer has a composite key on which it groups.
	 * 
	 * @param c The {@link ReduceStub} implementation for this Reduce InputContract.
	 * @param keyColumns The positions of the key fields in the input records.
	 */
	public ReduceContract(Class<? extends ReduceStub<?>> c, int[] keyColumns) {
		this(c, keyColumns, DEFAULT_NAME);
	}
	
	/**
	 * Creates a ReduceContract with the provided {@link ReduceStub} implementation 
	 * and the given name. 
	 * 
	 * @param c The {@link ReduceStub} implementation for this Reduce InputContract.
	 * @param keyColumn The position of the key in the input records.
	 * @param name The name of PACT.
	 */
	public ReduceContract(Class<? extends ReduceStub<?>> c, int keyColumn, String name) {
		this(c, new int[] {keyColumn}, name);
	}
	
	/**
	 * Creates a ReduceContract with the provided {@link ReduceStub} implementation 
	 * and the given name. The reducer has a composite key on which it groups.
	 * 
	 * @param c The {@link ReduceStub} implementation for this Reduce InputContract.
	 * @param keyColumns The positions of the key fields in the input records.
	 * @param name The name of PACT.
	 */
	public ReduceContract(Class<? extends ReduceStub<?>> c, int[] keyColumns, String name) {
		super(c, name);
		this.keyFields = keyColumns;
		this.keyClass = ReflectionUtil.getTemplateType(c, ReduceStub.class, 0);
	}

	/**
	 * Creates a ReduceContract with the provided {@link ReduceStub} implementation the default name.
	 * It uses the given contract as its input.
	 * 
	 * @param c The {@link ReduceStub} implementation for this Reduce InputContract.
	 * @param keyColumn The position of the key in the input records.
	 * @param input The contract to use as the input.
	 */
	public ReduceContract(Class<? extends ReduceStub<?>> c, int keyColumn, Contract input) {
		this(c, keyColumn, input, DEFAULT_NAME);
	}
	
	/**
	 * Creates a ReduceContract with the provided {@link ReduceStub} implementation the default name.
	 * It uses the given contract as its input. The reducer has a composite key on which it groups.
	 * 
	 * @param c The {@link ReduceStub} implementation for this Reduce InputContract.
	 * @param keyColumns The positions of the key fields in the input records.
	 * @param input The contract to use as the input.
	 */
	public ReduceContract(Class<? extends ReduceStub<?>> c, int[] keyColumns, Contract input) {
		this(c, keyColumns, input, DEFAULT_NAME);
	}
	
	/**
	 * Creates a ReduceContract with the provided {@link ReduceStub} implementation and the given name.
	 * It uses the given contract as its input.
	 * 
	 * @param c The {@link ReduceStub} implementation for this Reduce InputContract.
	 * @param keyColumn The position of the key in the input records.
	 * @param input The contract to use as the input.
	 * @param name The name of PACT.
	 */
	public ReduceContract(Class<? extends ReduceStub<?>> c, int keyColumn, Contract input, String name) {
		this(c, keyColumn, name);
		setInput(input);
	}
	
	/**
	 * Creates a ReduceContract with the provided {@link ReduceStub} implementation and the given name.
	 * It uses the given contract as its input. The reducer has a composite key on which it groups.
	 * 
	 * @param c The {@link ReduceStub} implementation for this Reduce InputContract.
	 * @param keyColumns The positions of the key fields in the input records.
	 * @param input The contract to use as the input.
	 * @param name The name of PACT.
	 */
	public ReduceContract(Class<? extends ReduceStub<?>> c, int[] keyColumns, Contract input, String name) {
		this(c, keyColumns, name);
		setInput(input);
	}
	
	/**
	 * Gets the column numbers of the key fields in the input records.
	 *  
	 * @return The column numbers of the key fields.
	 */
	public int[] getKeyColumnNumbers()
	{
		return this.keyFields;
	}
	
	/**
	 * Gets the type of the key field on which this reduce contract groups.
	 * 
	 * @return The type of the key field.
	 */
	public Class<? extends Key> getKeyClass()
	{
		return this.keyClass;
	}
	
	/**
	 * Sets the type of the key field on which this reduce contract groups.
	 * 
	 * @param clazz The type of the key field.
	 */
	public void setKeyClass(Class<? extends Key> clazz)
	{
		this.keyClass = clazz;
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Returns true if the ReduceContract is annotated with a Combinable annotation.
	 * The annotation indicates that the contract's {@link ReduceStub} implements the 
	 * {@link ReduceStub#combine(eu.stratosphere.pact.common.type.Key, java.util.Iterator, eu.stratosphere.pact.common.recordstubs.Collector)}
	 * method.
	 * 
	 * @return True, if the ReduceContract is combinable, false otherwise.
	 */
	public boolean isCombinable()
	{
		return getStubClass().getAnnotation(Combinable.class) != null;
	}
	
	/**
	 * This annotation marks reduce stubs as eligible for the usage of a combiner.
	 * 
	 * The following code excerpt shows how to make a simple reduce stub combinable (assuming here that
	 * the reducer function and combiner function do the same):
	 * 
	 * <code>
	 * \@Combinable
	 * public static class CountWords extends ReduceStub&lt;PactString&gt;
	 * {
	 *     private final PactInteger theInteger = new PactInteger();
	 * 
	 *     \@Override
	 *     public void reduce(PactString key, Iterator&lt;PactRecord&gt; records, Collector out) throws Exception
	 *     {
	 *         PactRecord element = null;
	 *         int sum = 0;
	 *         while (records.hasNext()) {
	 *             element = records.next();
	 *             element.getField(1, this.theInteger);
	 *             // we could have equivalently used PactInteger i = record.getField(1, PactInteger.class);
	 *          
	 *             sum += this.theInteger.getValue();
	 *         }
	 *      
	 *         element.setField(1, this.theInteger);
	 *         out.collect(element);
	 *     }
	 *     
	 *     public void combine(PactString key, Iterator&lt;PactRecord&gt; records, Collector out) throws Exception
	 *     {
	 *         this.reduce(key, records, out);
	 *     }
	 * }
	 * </code>
	 */
	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.TYPE)
	public @interface Combinable {};
}
