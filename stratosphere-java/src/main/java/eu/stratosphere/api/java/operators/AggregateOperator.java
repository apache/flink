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

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.aggregation.Aggregations;
import eu.stratosphere.api.java.operators.translation.UnaryNodeTranslation;

/**
 * @param <IN> The type of the data set aggregated by the operator.
 */
public class AggregateOperator<IN> extends SingleInputOperator<IN, IN, AggregateOperator<IN>> {
	
	private final Aggregations[] aggregationFunctions;
	
	private final int[] fields;
	
	private final int[] groupingFields;
	
	/**
	 * <p>
	 * This constructor merges this operator with the previous operator, if that
	 * one is an aggregation operator as well.
	 */
	public AggregateOperator(DataSet<IN> input, Aggregations function, int field) {
		super( (input != null && input.getClass() == AggregateOperator.class) ? ((AggregateOperator<IN>) input).getInput() : input , input.getType());
		
		if (input == null || function == null)
			throw new NullPointerException();
		
		if (field < 0 || field >= input.getType().getArity())
			throw new IllegalArgumentException("Field position is out of range.");
		
		// check if this is the first of multiple chained aggregation operators
		if (input.getClass() != AggregateOperator.class) {
			// this is the first aggregation operator after a regular data set (non grouped aggregation)
			this.aggregationFunctions = new Aggregations[] { function };
			this.fields = new int[] { field };
			this.groupingFields = new int[0];
		} else {
			// this aggregation operator succeeds another one, so merge them
			AggregateOperator<IN> pred = (AggregateOperator<IN>) input;
			int num = pred.aggregationFunctions.length;
			
			// copy the previous operators aggregation fields and add our own
			this.aggregationFunctions = new Aggregations[num + 1];
			this.fields = new int[num + 1];
			System.arraycopy(pred.aggregationFunctions, 0, this.aggregationFunctions, 0, num);
			System.arraycopy(pred.fields, 0, this.fields, 0, num);
			this.aggregationFunctions[num] = function;
			this.fields[num] = field;
			
			// copy the previous operator's grouping fields
			this.groupingFields = pred.groupingFields;
		}
	}
	
	public AggregateOperator(Grouping<IN> input, Aggregations function, int field) {
		super(input.getDataSet(), input.getDataSet().getType());
		
		if (input == null || function == null)
			throw new NullPointerException();
		
		if (field < 0 || field >= input.getDataSet().getType().getArity())
			throw new IllegalArgumentException("Aggregation field position is out of range.");
		
		// set the aggregation fields
		this.aggregationFunctions = new Aggregations[] { function };
		this.fields = new int[] { field };
		
		// get the grouping fields
		this.groupingFields = null; //input.getGroupingFields();
	}
	
	@Override
	protected UnaryNodeTranslation translateToDataFlow() {
		throw new UnsupportedOperationException("NOT IMPLEMENTED");
	}
}
