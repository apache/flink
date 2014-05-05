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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Iterator;

import eu.stratosphere.api.common.functions.AbstractFunction;
import eu.stratosphere.api.common.functions.GenericCombine;
import eu.stratosphere.api.common.functions.GenericGroupReduce;
import eu.stratosphere.util.Collector;

/**
 * Base class for user-defined group reduce functions.
 *
 * 
 * @param <IN> Object types of the incoming tuple/object stream
 * @param <OUT> Types of the elements returned by the user-defined function.
 */
public abstract class GroupReduceFunction<IN, OUT> extends AbstractFunction implements GenericGroupReduce<IN, OUT>, GenericCombine<IN> {
	
	private static final long serialVersionUID = 1L;

	@Override
	public abstract void reduce(Iterator<IN> values, Collector<OUT> out) throws Exception;
	
	@Override
	public void combine(Iterator<IN> values, Collector<IN> out) throws Exception {
		@SuppressWarnings("unchecked")
		Collector<OUT> c = (Collector<OUT>) out;
		reduce(values, c);
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.TYPE)
	public static @interface Combinable {};
}
