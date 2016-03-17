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

package org.apache.flink.api.java.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.operators.GenericDataSinkBase;
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.typeinfo.NothingTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.java.DataSet;

import java.util.Arrays;

@Public
public class DataSink<T> {
	
	private final OutputFormat<T> format;
	
	private final TypeInformation<T> type;
	
	private final DataSet<T> data;
	
	private String name;
	
	private int parallelism = -1;

	private Configuration parameters;

	private int[] sortKeyPositions;

	private Order[] sortOrders;

	public DataSink(DataSet<T> data, OutputFormat<T> format, TypeInformation<T> type) {
		if (format == null) {
			throw new IllegalArgumentException("The output format must not be null.");
		}
		if (type == null) {
			throw new IllegalArgumentException("The input type information must not be null.");
		}
		if (data == null) {
			throw new IllegalArgumentException("The data set must not be null.");
		}
		
		
		this.format = format;
		this.data = data;
		this.type = type;
	}


	@Internal
	public OutputFormat<T> getFormat() {
		return format;
	}

	@Internal
	public TypeInformation<T> getType() {
		return type;
	}

	@Internal
	public DataSet<T> getDataSet() {
		return data;
	}

	/**
	 * Pass a configuration to the OutputFormat
	 * @param parameters Configuration parameters
	 */
	public DataSink<T> withParameters(Configuration parameters) {
		this.parameters = parameters;
		return this;
	}

	/**
	 * Sorts each local partition of a {@link org.apache.flink.api.java.tuple.Tuple} data set
	 * on the specified field in the specified {@link Order} before it is emitted by the output format.<br>
	 * <b>Note: Only tuple data sets can be sorted using integer field indices.</b><br>
	 * The tuple data set can be sorted on multiple fields in different orders
	 * by chaining {@link #sortLocalOutput(int, Order)} calls.
	 *
	 * @param field The Tuple field on which the data set is locally sorted.
	 * @param order The Order in which the specified Tuple field is locally sorted.
	 * @return This data sink operator with specified output order.
	 *
	 * @see org.apache.flink.api.java.tuple.Tuple
	 * @see Order
	 */
	@Deprecated
	@Experimental
	public DataSink<T> sortLocalOutput(int field, Order order) {

		// get flat keys
		Keys.ExpressionKeys<T> ek = new Keys.ExpressionKeys<>(field, this.type);
		int[] flatKeys = ek.computeLogicalKeyPositions();

		if (!Keys.ExpressionKeys.isSortKey(field, this.type)) {
			throw new InvalidProgramException("Selected sort key is not a sortable type");
		}

		if(this.sortKeyPositions == null) {
			// set sorting info
			this.sortKeyPositions = flatKeys;
			this.sortOrders = new Order[flatKeys.length];
			Arrays.fill(this.sortOrders, order);
		} else {
			// append sorting info to exising info
			int oldLength = this.sortKeyPositions.length;
			int newLength = oldLength + flatKeys.length;
			this.sortKeyPositions = Arrays.copyOf(this.sortKeyPositions, newLength);
			this.sortOrders = Arrays.copyOf(this.sortOrders, newLength);

			for(int i=0; i<flatKeys.length; i++) {
				this.sortKeyPositions[oldLength+i] = flatKeys[i];
				this.sortOrders[oldLength+i] = order;
			}
		}

		return this;
	}

	/**
	 * Sorts each local partition of a data set on the field(s) specified by the field expression
	 * in the specified {@link Order} before it is emitted by the output format.<br>
	 * <b>Note: Non-composite types can only be sorted on the full element which is specified by
	 * a wildcard expression ("*" or "_").</b><br>
	 * Data sets of composite types (Tuple or Pojo) can be sorted on multiple fields in different orders
	 * by chaining {@link #sortLocalOutput(String, Order)} calls.
	 *
	 * @param fieldExpression The field expression for the field(s) on which the data set is locally sorted.
	 * @param order The Order in which the specified field(s) are locally sorted.
	 * @return This data sink operator with specified output order.
	 *
	 * @see Order
	 */
	@Deprecated
	@Experimental
	public DataSink<T> sortLocalOutput(String fieldExpression, Order order) {

		int numFields;
		int[] fields;
		Order[] orders;

		// compute flat field positions for (nested) sorting fields
		Keys.ExpressionKeys<T> ek = new Keys.ExpressionKeys<>(fieldExpression, this.type);
		fields = ek.computeLogicalKeyPositions();

		if (!Keys.ExpressionKeys.isSortKey(fieldExpression, this.type)) {
			throw new InvalidProgramException("Selected sort key is not a sortable type");
		}

		numFields = fields.length;
		orders = new Order[numFields];
		Arrays.fill(orders, order);

		if(this.sortKeyPositions == null) {
			// set sorting info
			this.sortKeyPositions = fields;
			this.sortOrders = orders;
		} else {
			// append sorting info to existing info
			int oldLength = this.sortKeyPositions.length;
			int newLength = oldLength + numFields;
			this.sortKeyPositions = Arrays.copyOf(this.sortKeyPositions, newLength);
			this.sortOrders = Arrays.copyOf(this.sortOrders, newLength);
			for(int i=0; i<numFields; i++) {
				this.sortKeyPositions[oldLength+i] = fields[i];
				this.sortOrders[oldLength+i] = orders[i];
			}
		}

		return this;
	}

	/**
	 * @return Configuration for the OutputFormat.
	 */
	public Configuration getParameters() {
		return this.parameters;
	}
	
	// --------------------------------------------------------------------------------------------
	
	public DataSink<T> name(String name) {
		this.name = name;
		return this;
	}
	
	// --------------------------------------------------------------------------------------------
	
	protected GenericDataSinkBase<T> translateToDataFlow(Operator<T> input) {
		// select the name (or create a default one)
		String name = this.name != null ? this.name : this.format.toString();
		GenericDataSinkBase<T> sink = new GenericDataSinkBase<>(this.format, new UnaryOperatorInformation<>(this.type, new NothingTypeInfo()), name);
		// set input
		sink.setInput(input);
		// set parameters
		if(this.parameters != null) {
			sink.getParameters().addAll(this.parameters);
		}
		// set parallelism
		if(this.parallelism > 0) {
			// use specified parallelism
			sink.setParallelism(this.parallelism);
		} else {
			// if no parallelism has been specified, use parallelism of input operator to enable chaining
			sink.setParallelism(input.getParallelism());
		}

		if(this.sortKeyPositions != null) {
			// configure output sorting
			Ordering ordering = new Ordering();
			for(int i=0; i<this.sortKeyPositions.length; i++) {
				ordering.appendOrdering(this.sortKeyPositions[i], null, this.sortOrders[i]);
			}
			sink.setLocalOrder(ordering);
		}
		
		return sink;
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return "DataSink '" + (this.name == null ? "<unnamed>" : this.name) + "' (" + this.format.toString() + ")";
	}
	
	/**
	 * Returns the parallelism of this data sink.
	 * 
	 * @return The parallelism of this data sink.
	 */
	public int getParallelism() {
		return this.parallelism;
	}
	
	/**
	 * Sets the parallelism for this data sink.
	 * The degree must be 1 or more.
	 * 
	 * @param parallelism The parallelism for this data sink.
	 * @return This data sink with set parallelism.
	 */
	public DataSink<T> setParallelism(int parallelism) {
		
		if(parallelism < 1) {
			throw new IllegalArgumentException("The parallelism of an operator must be at least 1.");
		}
		this.parallelism = parallelism;
		
		return this;
	}
}
