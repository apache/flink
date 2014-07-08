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

/**
 * Grouping is an intermediate step for a transformation on a grouped DataSet.<br/>
 * The following transformation can be applied on Grouping:
 * <ul>
 * 	<li>{@link Grouping#reduce(ReduceFunction)},</li>
 * <li>{@link Grouping#reduceGroup(GroupReduceFunction)}, and</li>
 * <li>{@link Grouping#aggregate(Aggregations, int)}.</li>
 * </ul>
 *
 * @param <T> The type of the elements of the grouped DataSet.
 * 
 * @see DataSet
 */
public abstract class Grouping<T> {
	
	protected final DataSet<T> dataSet;
	
	protected final Keys<T> keys;

	public Grouping(DataSet<T> set, Keys<T> keys) {
		if (set == null || keys == null) {
			throw new NullPointerException();
		}
		
		if (keys.isEmpty()) {
			throw new InvalidProgramException("The grouping keys must not be empty.");
		}

		this.dataSet = set;
		this.keys = keys;
	}
	
	
	protected DataSet<T> getDataSet() {
		return this.dataSet;
	}
	
	protected Keys<T> getKeys() {
		return this.keys;
	}

}
