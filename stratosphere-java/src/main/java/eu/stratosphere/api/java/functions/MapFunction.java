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
import eu.stratosphere.api.common.functions.GenericMap;

/**
 * The abstract base class for Map functions. Map functions take elements and transform them,
 * element wise. A Map function always produces a single result element for each input element.
 * Typical applications are parsing elements, converting data types, or projecting out fields.
 * Operations that produce multiple result elements from a single input element can be implemented
 * using the {@link FlatMapFunction}.
 * <p>
 * The basic syntax for using a MapFunction is as follows:
 * <pre><blockquote>
 * DataSet<X> input = ...;
 * 
 * DataSet<Y> result = input.map(new MyMapFunction());
 * </blockquote></pre>
 * <p>
 * Like all functions, the MapFunction needs to be serializable, as defined in {@link java.io.Serializable}.
 * 
 * @param <IN> Type of the input elements.
 * @param <OUT> Type of the returned elements.
 */
public abstract class MapFunction<IN, OUT> extends AbstractFunction implements GenericMap<IN, OUT> {

	private static final long serialVersionUID = 1L;

	/**
	 * The core method of the MapFunction. Takes an element from the input data set and transforms
	 * it into another element.
	 * 
	 * @param value The input value.
	 * @return The value produced by the map function from the input value.
	 * 
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
	 *                   to fail and may trigger recovery.
	 */
	@Override
	public abstract OUT map(IN value) throws Exception;
}
