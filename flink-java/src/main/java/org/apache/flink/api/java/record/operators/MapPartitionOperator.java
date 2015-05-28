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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.RecordOperator;
import org.apache.flink.api.common.operators.base.MapPartitionOperatorBase;
import org.apache.flink.api.common.operators.util.UserCodeClassWrapper;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.api.java.record.functions.FunctionAnnotation;
import org.apache.flink.api.java.record.functions.MapPartitionFunction;
import org.apache.flink.types.Key;
import org.apache.flink.types.Record;

import com.google.common.base.Preconditions;

/**
 * 
 *  <b>NOTE: The Record API is marked as deprecated. It is not being developed anymore and will be removed from
 * the code at some point.
 * See <a href="https://issues.apache.org/jira/browse/FLINK-1106">FLINK-1106</a> for more details.</b>
 * MapPartitionOperator that applies a {@link MapPartitionFunction} to each record independently.
 * 
 * @see MapPartitionFunction
 */

@Deprecated
public class MapPartitionOperator extends MapPartitionOperatorBase<Record, Record, MapPartitionFunction> implements RecordOperator {
	
	private static String DEFAULT_NAME = "<Unnamed MapPartition>";
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a Builder with the provided {@link MapPartitionFunction} implementation.
	 * 
	 * @param udf The {@link MapPartitionFunction} implementation for this Map operator.
	 */
	public static Builder builder(MapPartitionFunction udf) {
		return new Builder(new UserCodeObjectWrapper<MapPartitionFunction>(udf));
	}
	
	/**
	 * Creates a Builder with the provided {@link MapPartitionFunction} implementation.
	 * 
	 * @param udf The {@link MapPartitionFunction} implementation for this Map operator.
	 */
	public static Builder builder(Class<? extends MapPartitionFunction> udf) {
		return new Builder(new UserCodeClassWrapper<MapPartitionFunction>(udf));
	}
	
	/**
	 * The private constructor that only gets invoked from the Builder.
	 * @param builder
	 */
	protected MapPartitionOperator(Builder builder) {

		super(builder.udf, OperatorInfoHelper.unary(), builder.name);
		
		if (builder.inputs != null && !builder.inputs.isEmpty()) {
			setInput(Operator.createUnionCascade(builder.inputs));
		}
		
		setBroadcastVariables(builder.broadcastInputs);
		setSemanticProperties(FunctionAnnotation.readSingleConstantAnnotations(builder.udf));
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
		private final UserCodeWrapper<MapPartitionFunction> udf;
		
		/* The optional parameters */
		private List<Operator<Record>> inputs;
		private Map<String, Operator<Record>> broadcastInputs;
		private String name = DEFAULT_NAME;
		
		/**
		 * Creates a Builder with the provided {@link MapPartitionFunction} implementation.
		 * 
		 * @param udf The {@link MapPartitionFunction} implementation for this Map operator.
		 */
		private Builder(UserCodeWrapper<MapPartitionFunction> udf) {
			this.udf = udf;
			this.inputs = new ArrayList<Operator<Record>>();
			this.broadcastInputs = new HashMap<String, Operator<Record>>();
		}
		
		/**
		 * Sets the input.
		 * 
		 * @param input The input.
		 */
		public Builder input(Operator<Record> input) {
			Preconditions.checkNotNull(input, "The input must not be null");
			
			this.inputs.clear();
			this.inputs.add(input);
			return this;
		}
		
		/**
		 * Sets one or several inputs (union).
		 * 
		 * @param inputs
		 */
		public Builder input(Operator<Record>...inputs) {
			this.inputs.clear();
			for (Operator<Record> c : inputs) {
				this.inputs.add(c);
			}
			return this;
		}
		
		/**
		 * Sets the inputs.
		 * 
		 * @param inputs
		 */
		public Builder inputs(List<Operator<Record>> inputs) {
			this.inputs = inputs;
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
		 * Creates and returns a MapOperator from using the values given 
		 * to the builder.
		 * 
		 * @return The created operator
		 */
		public MapPartitionOperator build() {
			if (name == null) {
				name = udf.getUserCodeClass().getName();
			}
			return new MapPartitionOperator(this);
		}
	}
}
