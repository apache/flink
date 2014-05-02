/***********************************************************************************************************************
 *
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
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.java.operators;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.Validate;

import eu.stratosphere.api.common.InvalidProgramException;
import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.aggregation.AggregationFunction;
import eu.stratosphere.api.java.aggregation.AggregationFunctionFactory;
import eu.stratosphere.api.java.aggregation.Aggregations;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.operators.translation.PlanGroupReduceOperator;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.util.Collector;

/**
 * @param <IN> The type of the data set aggregated by the operator.
 */
public class AggregateOperator<IN> extends SingleInputOperator<IN, IN, AggregateOperator<IN>> {
	
	private final List<AggregationFunction<?>> aggregationFunctions = new ArrayList<AggregationFunction<?>>(4);
	
	private final List<Integer> fields = new ArrayList<Integer>(4);
	
	private final Grouping<IN> grouping;
	
	/**
	 * <p>
	 * Non grouped aggregation
	 */
	public AggregateOperator(DataSet<IN> input, Aggregations function, int field) {
		super(Validate.notNull(input), input.getType());
		
		Validate.notNull(function);
		
		if (!input.getType().isTupleType()) {
			throw new InvalidProgramException("Aggregating on field positions is only possible on tuple data types.");
		}
		
		TupleTypeInfo<?> inType = (TupleTypeInfo<?>) input.getType();
		
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
	 * 
	 * Grouped aggregation
	 * 
	 * @param input
	 * @param function
	 * @param field
	 */
	public AggregateOperator(Grouping<IN> input, Aggregations function, int field) {
		super(Validate.notNull(input).getDataSet(), input.getDataSet().getType());
		
		Validate.notNull(function);
		
		if (!input.getDataSet().getType().isTupleType()) {
			throw new InvalidProgramException("Aggregating on field positions is only possible on tuple data types.");
		}
		
		TupleTypeInfo<?> inType = (TupleTypeInfo<?>) input.getDataSet().getType();
		
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
	
	
	public AggregateOperator<IN> and(Aggregations function, int field) {
		Validate.notNull(function);
		
		TupleTypeInfo<?> inType = (TupleTypeInfo<?>) getType();
		
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
	protected Operator translateToDataFlow(Operator input) {
		
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
		genName.setLength(genName.length()-1);
		
		
		@SuppressWarnings("rawtypes")
		GroupReduceFunction<IN, IN> function = new AggregatingUdf(aggFunctions, fields);
		
		
		String name = getName() != null ? getName() : genName.toString();
		
		// distinguish between grouped reduce and non-grouped reduce
		if (this.grouping == null) {
			// non grouped aggregation
			PlanGroupReduceOperator<IN, IN> po = 
					new PlanGroupReduceOperator<IN, IN>(function, new int[0], name, getInputType(), getResultType());
			// set input
			po.setInput(input);
			// set dop
			po.setDegreeOfParallelism(this.getParallelism());
			
			return po;
		}
		
		if (this.grouping.getKeys() instanceof Keys.FieldPositionKeys) {
			// grouped aggregation
			int[] logicalKeyPositions = this.grouping.getKeys().computeLogicalKeyPositions();
			PlanGroupReduceOperator<IN, IN> po = 
					new PlanGroupReduceOperator<IN, IN>(function, logicalKeyPositions, name, getInputType(), getResultType());
			// set input
			po.setInput(input);
			// set dop
			po.setDegreeOfParallelism(this.getParallelism());
			
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
	// --------------------------------------------------------------------------------------------
	
	public static final class AggregatingUdf<T extends Tuple> extends GroupReduceFunction<T, T> {
		private static final long serialVersionUID = 1L;
		
		private final int[] fieldPositions;
		
		private final AggregationFunction<Object>[] aggFunctions;
		
		
		public AggregatingUdf(AggregationFunction<Object>[] aggFunctions, int[] fieldPositions) {
			Validate.notNull(aggFunctions);
			Validate.notNull(aggFunctions);
			Validate.isTrue(aggFunctions.length == fieldPositions.length);
			
			this.aggFunctions = aggFunctions;
			this.fieldPositions = fieldPositions;
		}
		

		@Override
		public void open(Configuration parameters) throws Exception {
			for (int i = 0; i < aggFunctions.length; i++) {
				aggFunctions[i].initializeAggregate();
			}
		}
		
		@Override
		public void reduce(Iterator<T> values, Collector<T> out) {
			final AggregationFunction<Object>[] aggFunctions = this.aggFunctions;
			final int[] fieldPositions = this.fieldPositions;

			// aggregators are initialized from before
			
			T current = null;
			while (values.hasNext()) {
				current = values.next();
				
				for (int i = 0; i < fieldPositions.length; i++) {
					Object val = current.getField(fieldPositions[i]);
					aggFunctions[i].aggregate(val);
				}
			}
			
			for (int i = 0; i < fieldPositions.length; i++) {
				Object aggVal = aggFunctions[i].getAggregate();
				current.setField(aggVal, fieldPositions[i]);
				aggFunctions[i].initializeAggregate();
			}
			
			out.collect(current);
		}
		
	}
}
