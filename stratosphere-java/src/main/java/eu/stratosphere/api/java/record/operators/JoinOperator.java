/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.api.java.record.operators;

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.common.operators.base.JoinOperatorBase;
import eu.stratosphere.api.common.operators.util.UserCodeClassWrapper;
import eu.stratosphere.api.common.operators.util.UserCodeObjectWrapper;
import eu.stratosphere.api.common.operators.util.UserCodeWrapper;
import eu.stratosphere.api.java.record.functions.JoinFunction;
import eu.stratosphere.types.Key;


/**
 * JoinOperator that applies a {@link JoinFunction} to each pair of records from both inputs
 * that have matching keys.
 * 
 * @see JoinStub
 */
public class JoinOperator extends JoinOperatorBase<JoinFunction> implements RecordOperator {
	
	private static String DEFAULT_NAME = "<Unnamed Join>";		// the default name for operations
	
	/**
	 * The types of the keys that the contract operates on.
	 */
	private final Class<? extends Key>[] keyTypes;
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a Builder with the provided {@link JoinFunction} implementation
	 * 
	 * @param udf The {@link JoinFunction} implementation for this join.
	 * @param keyClass The class of the key data type.
	 * @param keyColumn1 The position of the key in the first input's records.
	 * @param keyColumn2 The position of the key in the second input's records.
	 */
	public static Builder builder(JoinFunction udf, Class<? extends Key> keyClass, int keyColumn1, int keyColumn2) {
		return new Builder(new UserCodeObjectWrapper<JoinFunction>(udf), keyClass, keyColumn1, keyColumn2);
	}
	
	/**
	 * Creates a Builder with the provided {@link JoinFunction} implementation
	 * 
	 * @param udf The {@link JoinFunction} implementation for this Match contract.
	 * @param keyClass The class of the key data type.
	 * @param keyColumn1 The position of the key in the first input's records.
	 * @param keyColumn2 The position of the key in the second input's records.
	 */
	public static Builder builder(Class<? extends JoinFunction> udf, Class<? extends Key> keyClass, int keyColumn1, int keyColumn2) {
		return new Builder(new UserCodeClassWrapper<JoinFunction>(udf), keyClass, keyColumn1, keyColumn2);
	}
	
	/**
	 * The private constructor that only gets invoked from the Builder.
	 * @param builder
	 */
	protected JoinOperator(Builder builder) {
		super(builder.udf, builder.getKeyColumnsArray1(),
				builder.getKeyColumnsArray2(), builder.name);
		this.keyTypes = builder.getKeyClassesArray();
		setFirstInputs(builder.inputs1);
		setSecondInputs(builder.inputs2);
	}
	
	@Override
	public Class<? extends Key>[] getKeyClasses() {
		return this.keyTypes;
	}
	
	// --------------------------------------------------------------------------------------------
		
	/**
	 * Builder pattern, straight from Joshua Bloch's Effective Java (2nd Edition).
	 */
	public static class Builder {
		
		/* The required parameters */
		private final UserCodeWrapper<JoinFunction> udf;
		private final List<Class<? extends Key>> keyClasses;
		private final List<Integer> keyColumns1;
		private final List<Integer> keyColumns2;
		
		/* The optional parameters */
		private List<Operator> inputs1;
		private List<Operator> inputs2;
		private String name = DEFAULT_NAME;
		
		
		/**
		 * Creates a Builder with the provided {@link JoinFunction} implementation
		 * 
		 * @param udf The {@link JoinFunction} implementation for this Match contract.
		 * @param keyClass The class of the key data type.
		 * @param keyColumn1 The position of the key in the first input's records.
		 * @param keyColumn2 The position of the key in the second input's records.
		 */
		protected Builder(UserCodeWrapper<JoinFunction> udf, Class<? extends Key> keyClass, int keyColumn1, int keyColumn2) {
			this.udf = udf;
			this.keyClasses = new ArrayList<Class<? extends Key>>();
			this.keyClasses.add(keyClass);
			this.keyColumns1 = new ArrayList<Integer>();
			this.keyColumns1.add(keyColumn1);
			this.keyColumns2 = new ArrayList<Integer>();
			this.keyColumns2.add(keyColumn2);
			this.inputs1 = new ArrayList<Operator>();
			this.inputs2 = new ArrayList<Operator>();
		}
		
		/**
		 * Creates a Builder with the provided {@link JoinFunction} implementation. This method is intended 
		 * for special case sub-types only.
		 * 
		 * @param udf The {@link JoinFunction} implementation for this Match contract.
		 */
		protected Builder(UserCodeWrapper<JoinFunction> udf) {
			this.udf = udf;
			this.keyClasses = new ArrayList<Class<? extends Key>>();
			this.keyColumns1 = new ArrayList<Integer>();
			this.keyColumns2 = new ArrayList<Integer>();
			this.inputs1 = new ArrayList<Operator>();
			this.inputs2 = new ArrayList<Operator>();
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
		 * Sets one or several inputs (union) for input 1.
		 * 
		 * @param inputs
		 */
		public Builder input1(Operator ...inputs) {
			this.inputs1.clear();
			for (Operator c : inputs) {
				this.inputs1.add(c);
			}
			return this;
		}
		
		/**
		 * Sets one or several inputs (union) for input 2.
		 * 
		 * @param inputs
		 */
		public Builder input2(Operator ...inputs) {
			this.inputs2.clear();
			for (Operator c : inputs) {
				this.inputs2.add(c);
			}
			return this;
		}
		
		/**
		 * Sets the first inputs.
		 * 
		 * @param inputs
		 */
		public Builder inputs1(List<Operator> inputs) {
			this.inputs1 = inputs;
			return this;
		}
		
		/**
		 * Sets the second inputs.
		 * 
		 * @param inputs
		 */
		public Builder inputs2(List<Operator> inputs) {
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
		 * Creates and returns a JoinOperator from using the values given 
		 * to the builder.
		 * 
		 * @return The created contract
		 */
		public JoinOperator build() {
			if (keyClasses.size() <= 0) {
				throw new IllegalStateException("At least one key attribute has to be set.");
			}
			return new JoinOperator(this);
		}
	}
}
