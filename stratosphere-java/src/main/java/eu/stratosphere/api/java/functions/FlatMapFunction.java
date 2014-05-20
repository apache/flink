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
import eu.stratosphere.api.common.functions.GenericFlatMap;
import eu.stratosphere.util.Collector;

/**
 * The abstract base class for flatMap functions. FlatMap functions take elements and transform them,
 * into zero, one, or more elements. Typical applications can be splitting elements, or unnesting lists
 * and arrays. Operations that produce multiple strictly one result element per input element can also
 * use the {@link MapFunction}.
 * <p>
 * The basic syntax for using a FlatMapFunction is as follows:
 * <pre><blockquote>
 * DataSet<X> input = ...;
 * 
 * DataSet<Y> result = input.flatMap(new MyFlatMapFunction());
 * </blockquote></pre>
 * <p>
 * Like all functions, the FlatMapFunction needs to be serializable, as defined in {@link java.io.Serializable}.
 * 
 * @param <IN> Type of the input elements.
 * @param <OUT> Type of the returned elements.
 */
public abstract class FlatMapFunction<IN, OUT> extends AbstractFunction implements GenericFlatMap<IN, OUT> {

	private static final long serialVersionUID = 1L;

	/**
	 * The core method of the FlatMapFunction. Takes an element from the input data set and transforms
	 * it into zero, one, or more elements.
	 * 
	 * @param value The input value.
	 * @param out The collector for for emitting result values.
	 * 
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
	 *                   to fail and may trigger recovery.
	 */
	@Override
	public abstract void flatMap(IN value, Collector<OUT> out) throws Exception;
}
