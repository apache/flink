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

import java.util.Iterator;

import eu.stratosphere.api.common.functions.AbstractFunction;
import eu.stratosphere.api.common.functions.GenericCoGrouper;
import eu.stratosphere.api.java.operators.Keys;
import eu.stratosphere.util.Collector;

/**
 * The abstract base class for CoGroup functions. CoGroup functions combine two data sets by first grouping each data set
 * after a key and then "joining" the groups by calling this function with the two sets for each key.
 * <p>
 * The basic syntax for using CoGoup on two data sets is as follows:
 * <pre>
 * DataSet<X> set1 = ...;
 * DataSet<Y> set2 = ...;
 * 
 * set1.coGroup(set2).where(<key-definition>).equalTo(<key-definition>).with(new MyCoGroupFunction());
 * </pre>
 * The keys can be defined through tuple field positions or key extractors.
 * See {@link Keys} for details.
 * <p>
 * Some keys may only be contained in one of the two original data sets. In that case, the CoGroup function is invoked
 * with in empty input for the side of the data set that did not contain elements with that specific key.
 */
public abstract class CoGroupFunction<IN1, IN2, OUT> extends AbstractFunction implements GenericCoGrouper<IN1, IN2, OUT> {

	private static final long serialVersionUID = 1L;
	
	
	/**
	 * The core method of the CoGroupFunction. This method is called for each pair of groups that have the same
	 * key. The elements of the groups are returned by the respective iterators.
	 * 
	 *  It is possible that one of the two groups is empty, in which case the respective iterator has no elements.
	 */
	@Override
	public abstract void coGroup(Iterator<IN1> first, Iterator<IN2> second, Collector<OUT> out) throws Exception;

}
