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
package eu.stratosphere.api.java.functions;

import eu.stratosphere.api.common.functions.AbstractFunction;
import eu.stratosphere.api.common.functions.GenericReduce;

/**
 * Base class for a user-defined reduce function.
 * 
 * (For a MapReduce/Hadoop-style reduce, see {@link DataSet#reduceGroup(GroupReduceFunction)). 
 * 
 * @param <T> Object type to perform the reduction on.
 */
public abstract class ReduceFunction<T> extends AbstractFunction implements GenericReduce<T> {
	
	private static final long serialVersionUID = 1L;

	/**
	* Combines two values into one.
	* The reduce function is consecutively applied to all values of a group until only a single value remains.
	* In functional programming, this is known as a fold-style aggregation.
	*
	* Important: It is fine to return the second value object (value2) as result from this function.
	* You must NOT return the first value object (value1) from this function.
	*
	* @param value1 The first value to combine. This object must NOT be returned as result.
	* @param value2 The second value to combine. This object may be returned as result.
	* @return The combined value of both input values.
	*
	* @throws Exception
	*/
	public abstract T reduce(T value1, T value2) throws Exception;
}
