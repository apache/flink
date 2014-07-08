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
import eu.stratosphere.api.common.functions.GenericFilter;

/**
 * The abstract base class for Filter functions. A filter function take elements and evaluates a
 * predicate on them to decide whether to keep the element, or to discard it.
 * <p>
 * The basic syntax for using a FilterFunction is as follows:
 * <pre><blockquote>
 * DataSet<X> input = ...;
 * 
 * DataSet<X> result = input.filter(new MyFilterFunction());
 * </blockquote></pre>
 * <p>
 * Like all functions, the FilterFunction needs to be serializable, as defined in {@link java.io.Serializable}.
 * 
 * @param <T> The type of the filtered elements.
 */
public abstract class FilterFunction<T> extends AbstractFunction implements GenericFilter<T> {
	
	private static final long serialVersionUID = 1L;
	
	/**
	 * The core method of the FilterFunction. The method is called for each element in the input,
	 * and determines whether the element should be kept or filtered out. If the method returns true,
	 * the element passes the filter and is kept, if the method returns false, the element is
	 * filtered out.
	 * 
	 * @param value The input value to be filtered.
	 * @return Flag to indicate whether to keep the value (true) or to discard it (false).
	 * 
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
	 *                   to fail and may trigger recovery.
	 */
	@Override
	public abstract boolean filter(T value) throws Exception;
}
