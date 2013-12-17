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

package eu.stratosphere.api.operators.base;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import eu.stratosphere.api.functions.GenericReducer;
import eu.stratosphere.api.operators.SingleInputOperator;
import eu.stratosphere.api.operators.util.UserCodeClassWrapper;
import eu.stratosphere.api.operators.util.UserCodeObjectWrapper;
import eu.stratosphere.api.operators.util.UserCodeWrapper;


/**
 * ReduceContract represents a Pact with a Reduce Input Operator.
 * InputContracts are second-order functions. They have one or multiple input sets of records and a first-order
 * user function (stub implementation).
 * <p> 
 * Reduce works on a single input and calls the first-order user function of a {@link GenericReducer} for each group of 
 * records that share the same key.
 * 
 * @see GenericReducer
 */
public class ReduceOperatorBase<T extends GenericReducer<?, ?>> extends SingleInputOperator<T> {
	
	public ReduceOperatorBase(UserCodeWrapper<T> udf, int[] keyPositions, String name) {
		super(udf, keyPositions, name);
	}
	
	public ReduceOperatorBase(T udf, int[] keyPositions, String name) {
		super(new UserCodeObjectWrapper<T>(udf), keyPositions, name);
	}
	
	public ReduceOperatorBase(Class<? extends T> udf, int[] keyPositions, String name) {
		super(new UserCodeClassWrapper<T>(udf), keyPositions, name);
	}
	
	public ReduceOperatorBase(UserCodeWrapper<T> udf, String name) {
		super(udf, name);
	}
	
	public ReduceOperatorBase(T udf, String name) {
		super(new UserCodeObjectWrapper<T>(udf), name);
	}
	
	public ReduceOperatorBase(Class<? extends T> udf, String name) {
		super(new UserCodeClassWrapper<T>(udf), name);
	}

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Returns true if the ReduceContract is annotated with a Combinable annotation.
	 * The annotation indicates that the contract's {@link ReduceStub} implements the 
	 * {@link ReduceStub#combine(Iterator, Collector)}
	 * method.
	 *  
	 * @return True, if the ReduceContract is combinable, false otherwise.
	 */
	public boolean isCombinable() {
		return getUserCodeAnnotation(Combinable.class) != null;
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
	public static @interface Combinable {};
}
