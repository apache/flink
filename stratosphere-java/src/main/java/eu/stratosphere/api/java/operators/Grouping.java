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

import eu.stratosphere.api.common.InvalidProgramException;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.aggregation.Aggregations;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.functions.ReduceFunction;

public class Grouping<T> {
	
	private final DataSet<T> dataSet;
	
	private final Keys<T> keys;
	

	public Grouping(DataSet<T> set, Keys<T> keys) {
		if (set == null || keys == null)
			throw new NullPointerException();
		
		if (keys.isEmpty()) {
			throw new InvalidProgramException("The grouping keys must not be empty.");
		}

		this.dataSet = set;
		this.keys = keys;
	}
	
	
	public DataSet<T> getDataSet() {
		return this.dataSet;
	}
	
	public Keys<T> getKeys() {
		return this.keys;
	}
	
	// --------------------------------------------------------------------------------------------
	//  Operations / Transformations
	// --------------------------------------------------------------------------------------------
	
	public AggregateOperator<T> aggregate(Aggregations agg, int field) {
		return new AggregateOperator<T>(this, agg, field);
	}
	
	public ReduceOperator<T> reduce(ReduceFunction<T> reducer) {
		return new ReduceOperator<T>(this, reducer);
	}
	
	public <R> ReduceGroupOperator<T, R> reduceGroup(GroupReduceFunction<T, R> reducer) {
		return new ReduceGroupOperator<T, R>(this, reducer);
	}
	
	
}
