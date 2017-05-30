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

package org.apache.flink.api.scala.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.SingleInputSemanticProperties;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.base.GroupReduceOperatorBase;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.aggregation.AggregationFunction;
import org.apache.flink.api.java.aggregation.AggregationFunctionFactory;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.Grouping;
import org.apache.flink.api.java.operators.SingleInputOperator;
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializerBase;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;

import scala.Product;

/**
 * This operator represents the application of a "aggregate" operation on a data set, and the
 * result data set produced by the function.
 *
 * @param <IN> The type of the data set aggregated by the operator.
 */
@Public
public class ScalaAggregateOperator<IN> extends SingleInputOperator<IN, IN, ScalaAggregateOperator<IN>> {

	private final List<AggregationFunction<?>> aggregationFunctions = new ArrayList<>(4);

	private final List<Integer> fields = new ArrayList<>(4);

	private final Grouping<IN> grouping;

	/**
	 * Non grouped aggregation.
	 */
	public ScalaAggregateOperator(org.apache.flink.api.java.DataSet<IN> input, Aggregations function, int field) {
		super(Preconditions.checkNotNull(input), input.getType());

		Preconditions.checkNotNull(function);

		if (!input.getType().isTupleType()) {
			throw new InvalidProgramException("Aggregating on field positions is only possible on tuple data types.");
		}

		TupleTypeInfoBase<?> inType = (TupleTypeInfoBase<?>) input.getType();

		if (field < 0 || field >= inType.getArity()) {
			throw new IllegalArgumentException("Aggregation field position is out of range.");
		}

		AggregationFunctionFactory factory = function.getFactory();
		AggregationFunction<?> aggFunct = factory.createAggregationFunction(inType.getTypeAt(field).getTypeClass());

		// this is the first aggregation operator after a regular data set (non grouped aggregation)
		this.aggregationFunctions.add(aggFunct);
		this.fields.add(field);
		this.grouping = null;
	}

	/**
	 * Grouped aggregation.
	 *
	 * @param input
	 * @param function
	 * @param field
	 */
	public ScalaAggregateOperator(Grouping<IN> input, Aggregations function, int field) {
		super(Preconditions.checkNotNull(input).getInputDataSet(), input.getInputDataSet().getType());

		Preconditions.checkNotNull(function);

		if (!input.getInputDataSet().getType().isTupleType()) {
			throw new InvalidProgramException("Aggregating on field positions is only possible on tuple data types.");
		}

		TupleTypeInfoBase<?> inType = (TupleTypeInfoBase<?>) input.getInputDataSet().getType();

		if (field < 0 || field >= inType.getArity()) {
			throw new IllegalArgumentException("Aggregation field position is out of range.");
		}

		AggregationFunctionFactory factory = function.getFactory();
		AggregationFunction<?> aggFunct = factory.createAggregationFunction(inType.getTypeAt(field).getTypeClass());

		// set the aggregation fields
		this.aggregationFunctions.add(aggFunct);
		this.fields.add(field);
		this.grouping = input;
	}

	public ScalaAggregateOperator<IN> and(Aggregations function, int field) {
		Preconditions.checkNotNull(function);

		TupleTypeInfoBase<?> inType = (TupleTypeInfoBase<?>) getType();

		if (field < 0 || field >= inType.getArity()) {
			throw new IllegalArgumentException("Aggregation field position is out of range.");
		}

		AggregationFunctionFactory factory = function.getFactory();
		AggregationFunction<?> aggFunct = factory.createAggregationFunction(inType.getTypeAt(field).getTypeClass());

		this.aggregationFunctions.add(aggFunct);
		this.fields.add(field);

		return this;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected org.apache.flink.api.common.operators.base.GroupReduceOperatorBase<IN, IN, GroupReduceFunction<IN, IN>> translateToDataFlow(Operator<IN> input) {

		// sanity check
		if (this.aggregationFunctions.isEmpty() || this.aggregationFunctions.size() != this.fields.size()) {
			throw new IllegalStateException();
		}

		// construct the aggregation function
		AggregationFunction<Object>[] aggFunctions = new AggregationFunction[this.aggregationFunctions.size()];
		int[] fields = new int[this.fields.size()];
		StringBuilder genName = new StringBuilder();

		for (int i = 0; i < fields.length; i++) {
			aggFunctions[i] = (AggregationFunction<Object>) this.aggregationFunctions.get(i);
			fields[i] = this.fields.get(i);

			genName.append(aggFunctions[i].toString()).append('(').append(fields[i]).append(')').append(',');
		}
		genName.setLength(genName.length() - 1);

		@SuppressWarnings("rawtypes")
		RichGroupReduceFunction<IN, IN> function = new AggregatingUdf(getInputType(), aggFunctions, fields);

		String name = getName() != null ? getName() : genName.toString();

		// distinguish between grouped reduce and non-grouped reduce
		if (this.grouping == null) {
			// non grouped aggregation
			UnaryOperatorInformation<IN, IN> operatorInfo = new UnaryOperatorInformation<>(getInputType(), getResultType());
			GroupReduceOperatorBase<IN, IN, GroupReduceFunction<IN, IN>> po =
					new GroupReduceOperatorBase<IN, IN, GroupReduceFunction<IN, IN>>(function, operatorInfo, new int[0], name);

			po.setCombinable(true);

			// set input
			po.setInput(input);
			// set parallelism
			po.setParallelism(this.getParallelism());

			return po;
		}

		if (this.grouping.getKeys() instanceof Keys.ExpressionKeys) {
			// grouped aggregation
			int[] logicalKeyPositions = this.grouping.getKeys().computeLogicalKeyPositions();
			UnaryOperatorInformation<IN, IN> operatorInfo = new UnaryOperatorInformation<>(getInputType(), getResultType());
			GroupReduceOperatorBase<IN, IN, GroupReduceFunction<IN, IN>> po =
					new GroupReduceOperatorBase<IN, IN, GroupReduceFunction<IN, IN>>(function, operatorInfo, logicalKeyPositions, name);

			po.setCombinable(true);

			// set input
			po.setInput(input);
			// set parallelism
			po.setParallelism(this.getParallelism());

			SingleInputSemanticProperties props = new SingleInputSemanticProperties();

			for (int keyField : logicalKeyPositions) {
				boolean keyFieldUsedInAgg = false;
				for (int aggField : fields) {
					if (keyField == aggField) {
						keyFieldUsedInAgg = true;
						break;
					}
				}

				if (!keyFieldUsedInAgg) {
					props.addForwardedField(keyField, keyField);
				}
			}

			po.setSemanticProperties(props);
			po.setCustomPartitioner(grouping.getCustomPartitioner());

			return po;
		}
		else if (this.grouping.getKeys() instanceof Keys.SelectorFunctionKeys) {
			throw new UnsupportedOperationException("Aggregate does not support grouping with KeySelector functions, yet.");
		}
		else {
			throw new UnsupportedOperationException("Unrecognized key type.");
		}

	}

	// --------------------------------------------------------------------------------------------

	@Internal
	private static final class AggregatingUdf<T extends Product>
		extends RichGroupReduceFunction<T, T>
		implements GroupCombineFunction<T, T> {

		private static final long serialVersionUID = 1L;

		private final int[] fieldPositions;

		private final AggregationFunction<Object>[] aggFunctions;

		private TupleSerializerBase<T> serializer;

		private TypeInformation<T> typeInfo;

		public AggregatingUdf(TypeInformation<T> typeInfo, AggregationFunction<Object>[] aggFunctions, int[] fieldPositions) {
			Preconditions.checkNotNull(typeInfo);
			Preconditions.checkNotNull(aggFunctions);
			Preconditions.checkArgument(aggFunctions.length == fieldPositions.length);
			Preconditions.checkArgument(typeInfo.isTupleType(), "TypeInfo for Scala Aggregate Operator must be a tuple TypeInfo.");
			this.typeInfo = typeInfo;
			this.aggFunctions = aggFunctions;
			this.fieldPositions = fieldPositions;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			for (AggregationFunction<Object> aggFunction : aggFunctions) {
				aggFunction.initializeAggregate();
			}
			this.serializer = (TupleSerializerBase<T>) typeInfo.createSerializer(getRuntimeContext().getExecutionConfig());
		}

		@Override
		public void reduce(Iterable<T> records, Collector<T> out) {
			final AggregationFunction<Object>[] aggFunctions = this.aggFunctions;
			final int[] fieldPositions = this.fieldPositions;

			// aggregators are initialized from before

			T current = null;
			for (T record : records) {
				current = record;
				for (int i = 0; i < fieldPositions.length; i++) {
					Object val = current.productElement(fieldPositions[i]);
					aggFunctions[i].aggregate(val);
				}
			}

			Object[] fields = new Object[serializer.getArity()];
			int length = serializer.getArity();
			// First copy all tuple fields, then overwrite the aggregated ones
			for (int i = 0; i < length; i++) {
				fields[i] = current.productElement(i);
			}
			for (int i = 0; i < fieldPositions.length; i++) {
				Object aggVal = aggFunctions[i].getAggregate();
				fields[fieldPositions[i]] = aggVal;
				aggFunctions[i].initializeAggregate();
			}

			T result = serializer.createInstance(fields);

			out.collect(result);
		}

		@Override
		public void combine(Iterable<T> records, Collector<T> out) {
			reduce(records, out);
		}

	}
}
