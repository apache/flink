/**
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.Validate;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.RecordOperator;
import org.apache.flink.api.common.operators.base.CrossOperatorBase;
import org.apache.flink.api.common.operators.util.UserCodeClassWrapper;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.api.java.record.functions.CrossFunction;
import org.apache.flink.api.java.record.functions.FunctionAnnotation;
import org.apache.flink.types.Key;
import org.apache.flink.types.Record;


/**
 * CrossOperator that applies a {@link CrossFunction} to each element of the Cartesian Product.
 * 
 * @see CrossFunction
 */
public class CrossOperator extends CrossOperatorBase<Record, Record, Record, CrossFunction> implements RecordOperator {

	/**
	 * Creates a Builder with the provided {@link CrossFunction} implementation.
	 * 
	 * @param udf The {@link CrossFunction} implementation for this Cross operator.
	 */
	public static Builder builder(CrossFunction udf) {
		return new Builder(new UserCodeObjectWrapper<CrossFunction>(udf));
	}
	
	/**
	 * Creates a Builder with the provided {@link CrossFunction} implementation.
	 * 
	 * @param udf The {@link CrossFunction} implementation for this Cross operator.
	 */
	public static Builder builder(Class<? extends CrossFunction> udf) {
		return new Builder(new UserCodeClassWrapper<CrossFunction>(udf));
	}
	
	/**
	 * The private constructor that only gets eIinvoked from the Builder.
	 * @param builder
	 */
	protected CrossOperator(Builder builder) {
		super(builder.udf, OperatorInfoHelper.binary(), builder.name);
		
		if (builder.inputs1 != null && !builder.inputs1.isEmpty()) {
			setFirstInput(Operator.createUnionCascade(builder.inputs1));
		}
		if (builder.inputs2 != null && !builder.inputs2.isEmpty()) {
			setSecondInput(Operator.createUnionCascade(builder.inputs2));
		}
		
		setBroadcastVariables(builder.broadcastInputs);
		setSemanticProperties(FunctionAnnotation.readDualConstantAnnotations(builder.udf));
	}
	

	@Override
	public Class<? extends Key<?>>[] getKeyClasses() {
		return emptyClassArray();
	}
	
	// --------------------------------------------------------------------------------------------

	/**
	 * Builder pattern, straight from Joshua Bloch's Effective Java (2nd Edition).
	 */
	public static class Builder {
		
		/* The required parameters */
		private final UserCodeWrapper<CrossFunction> udf;
		
		/* The optional parameters */
		private List<Operator<Record>> inputs1;
		private List<Operator<Record>> inputs2;
		private Map<String, Operator<Record>> broadcastInputs;
		private String name;
		
		/**
		 * Creates a Builder with the provided {@link CrossFunction} implementation.
		 * 
		 * @param udf The {@link CrossFunction} implementation for this Cross operator.
		 */
		protected Builder(UserCodeWrapper<CrossFunction> udf) {
			this.udf = udf;
			this.inputs1 = new ArrayList<Operator<Record>>();
			this.inputs2 = new ArrayList<Operator<Record>>();
			this.broadcastInputs = new HashMap<String, Operator<Record>>();
		}
		
		/**
		 * Sets the first input.
		 * 
		 * @param input The first input.
		 */
		public Builder input1(Operator<Record> input) {
			Validate.notNull(input, "The input must not be null");
			
			this.inputs1.clear();
			this.inputs1.add(input);
			return this;
		}
		
		/**
		 * Sets the second input.
		 * 
		 * @param input The second input.
		 */
		public Builder input2(Operator<Record> input) {
			Validate.notNull(input, "The input must not be null");
			
			this.inputs2.clear();
			this.inputs2.add(input);
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
		 * Creates and returns a CrossOperator from using the values given 
		 * to the builder.
		 * 
		 * @return The created operator
		 */
		public CrossOperator build() {
			setNameIfUnset();
			return new CrossOperator(this);
		}
		
		protected void setNameIfUnset() {
			if (name == null) {
				name = udf.getUserCodeClass().getName();
			}
		}
	}
}
