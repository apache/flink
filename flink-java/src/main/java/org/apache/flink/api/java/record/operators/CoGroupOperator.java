/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.java.record.operators;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.Validate;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.RecordOperator;
import org.apache.flink.api.common.operators.base.CoGroupOperatorBase;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.api.java.operators.translation.WrappingFunction;
import org.apache.flink.api.java.record.functions.CoGroupFunction;
import org.apache.flink.api.java.record.functions.FunctionAnnotation;
import org.apache.flink.types.Key;
import org.apache.flink.types.Record;
import org.apache.flink.util.Collector;
import org.apache.flink.util.InstantiationUtil;

/**
 * CoGroupOperator that applies a {@link CoGroupFunction} to groups of records sharing
 * the same key (one group per input).
 * 
 * @see CoGroupFunction
 */
public class CoGroupOperator extends CoGroupOperatorBase<Record, Record, Record, org.apache.flink.api.common.functions.CoGroupFunction<Record, Record, Record>> implements RecordOperator {
	
	/**
	 * The types of the keys that the operator groups on.
	 */
	private final Class<? extends Key<?>>[] keyTypes;
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a Builder with the provided {@link CoGroupFunction} implementation.
	 * 
	 * @param udf The {@link CoGroupFunction} implementation for this CoGroup operator.
	 * @param keyClass The class of the key data type.
	 * @param keyColumn1 The position of the key in the first input's records.
	 * @param keyColumn2 The position of the key in the second input's records.
	 */
	public static Builder builder(CoGroupFunction udf, Class<? extends Key<?>> keyClass, int keyColumn1, int keyColumn2) {
		WrappingCoGroupFunction wrapper = new WrappingCoGroupFunction(udf);
		return new Builder(new UserCodeObjectWrapper<org.apache.flink.api.common.functions.CoGroupFunction<Record, Record, Record>>(wrapper), keyClass, keyColumn1, keyColumn2);
	}
	
	/**
	 * Creates a Builder with the provided {@link CoGroupFunction} implementation.
	 * 
	 * @param udf The {@link CoGroupFunction} implementation for this CoGroup operator.
	 * @param keyClass The class of the key data type.
	 * @param keyColumn1 The position of the key in the first input's records.
	 * @param keyColumn2 The position of the key in the second input's records.
	 */
	public static Builder builder(Class<? extends CoGroupFunction> udf, Class<? extends Key<?>> keyClass,
			int keyColumn1, int keyColumn2)
	{
		WrappingCoGroupFunction wrapper = new WrappingClassCoGroupFunction(udf);
		return new Builder(new UserCodeObjectWrapper<org.apache.flink.api.common.functions.CoGroupFunction<Record, Record, Record>>(wrapper), keyClass, keyColumn1, keyColumn2);
	}
	
	/**
	 * The private constructor that only gets invoked from the Builder.
	 * @param builder
	 */
	protected CoGroupOperator(Builder builder) {
		super(builder.udf, OperatorInfoHelper.binary(), builder.getKeyColumnsArray1(), builder.getKeyColumnsArray2(), builder.name);
		this.keyTypes = builder.getKeyClassesArray();
		
		if (builder.inputs1 != null && !builder.inputs1.isEmpty()) {
			setFirstInput(Operator.createUnionCascade(builder.inputs1));
		}
		if (builder.inputs2 != null && !builder.inputs2.isEmpty()) {
			setSecondInput(Operator.createUnionCascade(builder.inputs2));
		}

		// sanity check solution set key mismatches
		if (input1 instanceof DeltaIteration.SolutionSetPlaceHolder) {
			int[] positions = getKeyColumns(0);
			((DeltaIteration.SolutionSetPlaceHolder) input1).checkJoinKeyFields(positions);
		}
		if (input2 instanceof DeltaIteration.SolutionSetPlaceHolder) {
			int[] positions = getKeyColumns(1);
			((DeltaIteration.SolutionSetPlaceHolder) input2).checkJoinKeyFields(positions);
		}
		
		setBroadcastVariables(builder.broadcastInputs);
		setGroupOrderForInputOne(builder.secondaryOrder1);
		setGroupOrderForInputTwo(builder.secondaryOrder2);
		
		CoGroupFunction function = ((WrappingCoGroupFunction) builder.udf.getUserCodeObject()).getWrappedFunction();
		setSemanticProperties(FunctionAnnotation.readDualConstantAnnotations(new UserCodeObjectWrapper<CoGroupFunction>(function)));
	}

	// --------------------------------------------------------------------------------------------
	
	@Override
	public Class<? extends Key<?>>[] getKeyClasses() {
		return this.keyTypes;
	}
	
	// --------------------------------------------------------------------------------------------

	
	/**
	 * Builder pattern, straight from Joshua Bloch's Effective Java (2nd Edition).
	 */
	public static class Builder {
		
		/* The required parameters */
		private final UserCodeWrapper<org.apache.flink.api.common.functions.CoGroupFunction<Record, Record, Record>> udf;
		private final List<Class<? extends Key<?>>> keyClasses;
		private final List<Integer> keyColumns1;
		private final List<Integer> keyColumns2;
		
		/* The optional parameters */
		private List<Operator<Record>> inputs1;
		private List<Operator<Record>> inputs2;
		private Map<String, Operator<Record>> broadcastInputs;
		private Ordering secondaryOrder1;
		private Ordering secondaryOrder2;
		private String name;
		
		/**
		 * Creates a Builder with the provided {@link CoGroupFunction} implementation.
		 * 
		 * @param udf The {@link CoGroupFunction} implementation for this CoGroup operator.
		 * @param keyClass The class of the key data type.
		 * @param keyColumn1 The position of the key in the first input's records.
		 * @param keyColumn2 The position of the key in the second input's records.
		 */
		protected Builder(UserCodeWrapper<org.apache.flink.api.common.functions.CoGroupFunction<Record, Record, Record>> udf, Class<? extends Key<?>> keyClass,
				int keyColumn1, int keyColumn2)
		{
			this.udf = udf;
			this.keyClasses = new ArrayList<Class<? extends Key<?>>>();
			this.keyClasses.add(keyClass);
			this.keyColumns1 = new ArrayList<Integer>();
			this.keyColumns1.add(keyColumn1);
			this.keyColumns2 = new ArrayList<Integer>();
			this.keyColumns2.add(keyColumn2);
			this.inputs1 = new ArrayList<Operator<Record>>();
			this.inputs2 = new ArrayList<Operator<Record>>();
			this.broadcastInputs = new HashMap<String, Operator<Record>>();
		}
		
		/**
		 * Creates a Builder with the provided {@link CoGroupFunction} implementation. This method is intended
		 * for special case sub-types only.
		 * 
		 * @param udf The {@link CoGroupFunction} implementation for this CoGroup operator.
		 */
		protected Builder(UserCodeWrapper<org.apache.flink.api.common.functions.CoGroupFunction<Record, Record, Record>> udf) {
			this.udf = udf;
			this.keyClasses = new ArrayList<Class<? extends Key<?>>>();
			this.keyColumns1 = new ArrayList<Integer>();
			this.keyColumns2 = new ArrayList<Integer>();
			this.inputs1 = new ArrayList<Operator<Record>>();
			this.inputs2 = new ArrayList<Operator<Record>>();
			this.broadcastInputs = new HashMap<String, Operator<Record>>();
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
		private Class<? extends Key<?>>[] getKeyClassesArray() {
			return keyClasses.toArray(new Class[keyClasses.size()]);
		}

		/**
		 * Adds additional key field.
		 * 
		 * @param keyClass The class of the key data type.
		 * @param keyColumn1 The position of the key in the first input's records.
		 * @param keyColumn2 The position of the key in the second input's records.
		 */
		public Builder keyField(Class<? extends Key<?>> keyClass, int keyColumn1, int keyColumn2) {
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
		 * Sets the input operator for input 1.
		 * 
		 * @param input The input operator for input 1. 
		 */
		public Builder input1(Operator<Record> input) {
			Validate.notNull(input, "The input must not be null");
			
			this.inputs1.clear();
			this.inputs1.add(input);
			return this;
		}
		
		/**
		 * Sets one or several inputs (union) for input 1.
		 * 
		 * @param inputs
		 */
		public Builder input1(Operator<Record>...inputs) {
			this.inputs1.clear();
			for (Operator<Record> c : inputs) {
				this.inputs1.add(c);
			}
			return this;
		}
		
		/**
		 * Sets the input operator for input 2.
		 * 
		 * @param input The input operator for input 2. 
		 */
		public Builder input2(Operator<Record> input) {
			Validate.notNull(input, "The input must not be null");
			
			this.inputs2.clear();
			this.inputs2.add(input);
			return this;
		}
		
		/**
		 * Sets one or several inputs (union) for input 2.
		 * 
		 * @param inputs
		 */
		public Builder input2(Operator<Record>...inputs) {
			this.inputs2.clear();
			for (Operator<Record> c : inputs) {
				this.inputs2.add(c);
			}
			return this;
		}
		
		/**
		 * Sets the first inputs.
		 * 
		 * @param inputs
		 */
		public Builder inputs1(List<Operator<Record>> inputs) {
			this.inputs1 = inputs;
			return this;
		}
		
		/**
		 * Sets the second inputs.
		 * 
		 * @param inputs
		 */
		public Builder inputs2(List<Operator<Record>> inputs) {
			this.inputs2 = inputs;
			return this;
		}
		
		/**
		 * Binds the result produced by a plan rooted at {@code root} to a 
		 * variable used by the UDF wrapped in this operator.
		 */
		public Builder setBroadcastVariable(String name, Operator<Record> input) {
			this.broadcastInputs.put(name, input);
			return this;
		}
		
		/**
		 * Binds multiple broadcast variables.
		 */
		public Builder setBroadcastVariables(Map<String, Operator<Record>> inputs) {
			this.broadcastInputs.clear();
			this.broadcastInputs.putAll(inputs);
			return this;
		}
		
		/**
		 * Sets the name of this operator.
		 * 
		 * @param name
		 */
		public Builder name(String name) {
			this.name = name;
			return this;
		}
		
		/**
		 * Creates and returns a CoGroupOperator from using the values given 
		 * to the builder.
		 * 
		 * @return The created operator
		 */
		public CoGroupOperator build() {
			if (keyClasses.size() <= 0) {
				throw new IllegalStateException("At least one key attribute has to be set.");
			}
			
			if (name == null) {
				name = udf.getUserCodeClass().getName();
			}
			return new CoGroupOperator(this);
		}
	}
	
	// ============================================================================================
	
	public static class WrappingCoGroupFunction extends WrappingFunction<CoGroupFunction> 
			implements org.apache.flink.api.common.functions.CoGroupFunction<Record, Record, Record> {
		
		private static final long serialVersionUID = 1L;
		
		public WrappingCoGroupFunction(CoGroupFunction coGrouper) {
			super(coGrouper);
		}
		
		@Override
		public void coGroup(Iterable<Record> records1, Iterable<Record> records2, Collector<Record> out) throws Exception {
			this.wrappedFunction.coGroup(records1.iterator(), records2.iterator(), out);
		}
	}
	
	public static final class WrappingClassCoGroupFunction extends WrappingCoGroupFunction {
		
		private static final long serialVersionUID = 1L;
		
		public WrappingClassCoGroupFunction(Class<? extends CoGroupFunction> reducer) {
			super(InstantiationUtil.instantiate(reducer));
		}
		
		private void writeObject(ObjectOutputStream out) throws IOException {
			out.writeObject(wrappedFunction.getClass());
		}

		private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
			Class<?> clazz = (Class<?>) in.readObject();
			this.wrappedFunction = (CoGroupFunction) InstantiationUtil.instantiate(clazz);
		}
	}
}
