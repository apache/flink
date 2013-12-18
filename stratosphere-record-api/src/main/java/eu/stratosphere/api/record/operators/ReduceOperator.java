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

package eu.stratosphere.api.record.operators;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.api.operators.Operator;
import eu.stratosphere.api.operators.Ordering;
import eu.stratosphere.api.operators.base.ReduceOperatorBase;
import eu.stratosphere.api.operators.util.UserCodeClassWrapper;
import eu.stratosphere.api.operators.util.UserCodeObjectWrapper;
import eu.stratosphere.api.operators.util.UserCodeWrapper;
import eu.stratosphere.api.record.functions.ReduceFunction;
import eu.stratosphere.types.Key;

/**
 * ReduceOperator represents a Pact with a Reduce Input Operator.
 * InputContracts are second-order functions. They have one or multiple input sets of records and a first-order
 * user function (stub implementation).
 * <p> 
 * Reduce works on a single input and calls the first-order user function of a {@link ReduceFunction} for each group of 
 * records that share the same key.
 * 
 * @see ReduceFunction
 */
public class ReduceOperator extends ReduceOperatorBase<ReduceFunction> implements RecordOperator {
	
	private static final String DEFAULT_NAME = "<Unnamed Reducer>";		// the default name for contracts
	
	/**
	 * The types of the keys that the contract operates on.
	 */
	private final Class<? extends Key>[] keyTypes;
	
	/**
	 * The ordering for the order inside a reduce group.
	 */
	private Ordering groupOrder;
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a Builder with the provided {@link ReduceFunction} implementation.
	 * 
	 * @param udf The {@link ReduceFunction} implementation for this Reduce contract.
	 */
	public static Builder builder(ReduceFunction udf) {
		return new Builder(new UserCodeObjectWrapper<ReduceFunction>(udf));
	}
	
	/**
	 * Creates a Builder with the provided {@link ReduceFunction} implementation.
	 * 
	 * @param udf The {@link ReduceFunction} implementation for this Reduce contract.
	 * @param keyClass The class of the key data type.
	 * @param keyColumn The position of the key.
	 */
	public static Builder builder(ReduceFunction udf, Class<? extends Key> keyClass, int keyColumn) {
		return new Builder(new UserCodeObjectWrapper<ReduceFunction>(udf), keyClass, keyColumn);
	}

	/**
	 * Creates a Builder with the provided {@link ReduceFunction} implementation.
	 * 
	 * @param udf The {@link ReduceFunction} implementation for this Reduce contract.
	 */
	public static Builder builder(Class<? extends ReduceFunction> udf) {
		return new Builder(new UserCodeClassWrapper<ReduceFunction>(udf));
	}
	
	/**
	 * Creates a Builder with the provided {@link ReduceFunction} implementation.
	 * 
	 * @param udf The {@link ReduceFunction} implementation for this Reduce contract.
	 * @param keyClass The class of the key data type.
	 * @param keyColumn The position of the key.
	 */
	public static Builder builder(Class<? extends ReduceFunction> udf, Class<? extends Key> keyClass, int keyColumn) {
		return new Builder(new UserCodeClassWrapper<ReduceFunction>(udf), keyClass, keyColumn);
	}
	
	/**
	 * The private constructor that only gets invoked from the Builder.
	 * @param builder
	 */
	protected ReduceOperator(Builder builder) {
		super(builder.udf, builder.getKeyColumnsArray(), builder.name);
		this.keyTypes = builder.getKeyClassesArray();
		setInputs(builder.inputs);
		setGroupOrder(builder.secondaryOrder);
	}
	
	// --------------------------------------------------------------------------------------------
	

	@Override
	public Class<? extends Key>[] getKeyClasses() {
		return this.keyTypes;
	}
	
	/**
	 * Sets the order of the elements within a reduce group.
	 * 
	 * @param order The order for the elements in a reduce group.
	 */
	public void setGroupOrder(Ordering order) {
		this.groupOrder = order;
	}

	/**
	 * Gets the order of elements within a reduce group. If no such order has been
	 * set, this method returns null.
	 * 
	 * @return The secondary order.
	 */
	public Ordering getGroupOrder() {
		return this.groupOrder;
	}
	
	// --------------------------------------------------------------------------------------------
	

	@Override
	public boolean isCombinable() {
		return super.isCombinable() || getUserCodeAnnotation(Combinable.class) != null;
	}
	
	/**
	 * This annotation marks reduce stubs as eligible for the usage of a combiner.
	 * 
	 * The following code excerpt shows how to make a simple reduce stub combinable (assuming here that
	 * the reducer function and combiner function do the same):
	 * 
	 * <code>
	 * \@Combinable
	 * public static class CountWords extends ReduceFunction&lt;PactString&gt;
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
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Builder pattern, straight from Joshua Bloch's Effective Java (2nd Edition).
	 */
	public static class Builder {
		
		/* The required parameters */
		private final UserCodeWrapper<ReduceFunction> udf;
		private final List<Class<? extends Key>> keyClasses;
		private final List<Integer> keyColumns;
		
		/* The optional parameters */
		private Ordering secondaryOrder = null;
		private List<Operator> inputs;
		private String name = DEFAULT_NAME;
		
		/**
		 * Creates a Builder with the provided {@link ReduceFunction} implementation.
		 * 
		 * @param udf The {@link ReduceFunction} implementation for this Reduce contract.
		 */
		private Builder(UserCodeWrapper<ReduceFunction> udf) {
			this.udf = udf;
			this.keyClasses = new ArrayList<Class<? extends Key>>();
			this.keyColumns = new ArrayList<Integer>();
			this.inputs = new ArrayList<Operator>();
		}
		
		/**
		 * Creates a Builder with the provided {@link ReduceFunction} implementation.
		 * 
		 * @param udf The {@link ReduceFunction} implementation for this Reduce contract.
		 * @param keyClass The class of the key data type.
		 * @param keyColumn The position of the key.
		 */
		private Builder(UserCodeWrapper<ReduceFunction> udf, Class<? extends Key> keyClass, int keyColumn) {
			this.udf = udf;
			this.keyClasses = new ArrayList<Class<? extends Key>>();
			this.keyClasses.add(keyClass);
			this.keyColumns = new ArrayList<Integer>();
			this.keyColumns.add(keyColumn);
			this.inputs = new ArrayList<Operator>();
		}
		
		private int[] getKeyColumnsArray() {
			int[] result = new int[keyColumns.size()];
			for (int i = 0; i < keyColumns.size(); ++i) {
				result[i] = keyColumns.get(i);
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
		 * @param keyColumn The position of the key.
		 */
		public Builder keyField(Class<? extends Key> keyClass, int keyColumn) {
			keyClasses.add(keyClass);
			keyColumns.add(keyColumn);
			return this;
		}
		
		/**
		 * Sets the order of the elements within a group.
		 * 
		 * @param order The order for the elements in a group.
		 */
		public Builder secondaryOrder(Ordering order) {
			this.secondaryOrder = order;
			return this;
		}
		
		/**
		 * Sets one or several inputs (union).
		 * 
		 * @param inputs
		 */
		public Builder input(Operator ...inputs) {
			this.inputs.clear();
			for (Operator c : inputs) {
				this.inputs.add(c);
			}
			return this;
		}
		
		/**
		 * Sets the inputs.
		 * 
		 * @param inputs
		 */
		public Builder inputs(List<Operator> inputs) {
			this.inputs = inputs;
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
		 * Creates and returns a ReduceOperator from using the values given 
		 * to the builder.
		 * 
		 * @return The created contract
		 */
		public ReduceOperator build() {
			return new ReduceOperator(this);
		}
	}
}
