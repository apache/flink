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
	
	private static String DEFAULT_NAME = "<Unnamed Reducer>";

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a ReduceContract with the provided {@link ReduceStub} implementation
	 * and a default name.
	 * 
	 * @param c The {@link ReduceStub} implementation for this Reduce InputContract.
	 */
	public ReduceContract(Class<? extends ReduceStub<?>> c) {
		super(c, DEFAULT_NAME);
	}
	
	/**
	 * Creates a ReduceContract with the provided {@link ReduceStub} implementation 
	 * and the given name. 
	 * 
	 * @param c The {@link ReduceStub} implementation for this Reduce InputContract.
	 * @param name The name of PACT.
	 */
	public ReduceContract(Class<? extends ReduceStub<?>> c, String name) {
		super(c, name);
	}

	/**
	 * Creates a ReduceContract with the provided {@link ReduceStub} implementation the default name.
	 * It uses the given contract as its input.
	 * 
	 * @param c The {@link ReduceStub} implementation for this Reduce InputContract.
	 * @param input The contract to use as the input.
	 */
	public ReduceContract(Class<? extends ReduceStub<?>> c, Contract input) {
		this(c, DEFAULT_NAME);
		setInput(input);
	}
	
	/**
	 * Creates a ReduceContract with the provided {@link ReduceStub} implementation and the given name.
	 * It uses the given contract as its input.
	 * 
	 * @param c The {@link ReduceStub} implementation for this Reduce InputContract.
	 * @param input The contract to use as the input.
	 * @param name The name of PACT.
	 */
	public ReduceContract(Class<? extends ReduceStub<?>> c, Contract input, String name) {
		this(c, name);
		setInput(input);
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
}
