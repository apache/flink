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
 * The abstract base class for group reduce functions. Group reduce functions process groups of elements.
 * They may aggregate them to a single value, or produce multiple result values for each group.
 * <p>
 * For a reduce functions that works incrementally by combining always two elements, see 
 * {@link ReduceFunction}, called via {@link eu.stratosphere.api.java.DataSet#reduce(ReduceFunction)}.
 * <p>
 * The basic syntax for using a grouped GroupReduceFunction is as follows:
 * <pre><blockquote>
 * DataSet<X> input = ...;
 * 
 * DataSet<X> result = input.groupBy(<key-definition>).reduceGroup(new MyGroupReduceFunction());
 * </blockquote></pre>
 * <p>
 * GroupReduceFunctions may be "combinable", in which case they can pre-reduce partial groups in order to
 * reduce the data volume early. See the {@link #combine(Iterator, Collector)} function for details.
 * <p>
 * Like all functions, the GroupReduceFunction needs to be serializable, as defined in {@link java.io.Serializable}.
 * 
 * @param <IN> Type of the elements that this function processes.
 * @param <OUT> The type of the elements returned by the user-defined function.
 */
public abstract class GroupReduceFunction<IN, OUT> extends AbstractFunction implements GenericGroupReduce<IN, OUT>, GenericCombine<IN> {
	
	private static final long serialVersionUID = 1L;

	/**
	 * Core method of the reduce function. It is called one per group of elements. If the reducer
	 * is not grouped, than the entire data set is considered one group.
	 * 
	 * @param values The iterator returning the group of values to be reduced.
	 * @param out The collector to emit the returned values.
	 * 
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
	 *                   to fail and may trigger recovery.
	 */
	@Override
	public abstract void reduce(Iterator<IN> values, Collector<OUT> out) throws Exception;
	
	/**
	 * The combine methods pre-reduces elements. It may be called on subsets of the data
	 * before the actual reduce function. This is often helpful to lower data volume prior
	 * to reorganizing the data in an expensive way, as might be required for the final
	 * reduce function.
	 * <p>
	 * This method is only ever invoked when the subclass of {@link GroupReduceFunction}
	 * adds the {@link Combinable} annotation, or if the <i>combinable</i> flag is set when defining
	 * the <i>reduceGroup<i> operation via
	 * {@link eu.stratosphere.api.java.operators.ReduceGroupOperator#setCombinable(boolean)}.
	 * <p>
	 * Since the reduce function will be called on the result of this method, it is important that this
	 * method returns the same data type as it consumes. By default, this method only calls the
	 * {@link #reduce(Iterator, Collector)} method. If the behavior in the pre-reducing is different
	 * from the final reduce function (for example because the reduce function changes the data type),
	 * this method must be overwritten, or the execution will fail.
	 * 
	 * @param values The iterator returning the group of values to be reduced.
	 * @param out The collector to emit the returned values.
	 * 
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
	 *                   to fail and may trigger recovery.
	 */
	@Override
	public void combine(Iterator<IN> values, Collector<IN> out) throws Exception {
		@SuppressWarnings("unchecked")
		Collector<OUT> c = (Collector<OUT>) out;
		reduce(values, c);
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * This annotation can be added to classes that extend {@link GroupReduceFunction}, in oder to mark
	 * them as "combinable". The system may call the {@link GroupReduceFunction#combine(Iterator, Collector)}
	 * method on such functions, to pre-reduce the data before transferring it over the network to
	 * the actual group reduce operation.
	 * <p>
	 * Marking combinable functions as such is in general beneficial for performance.
	 */
	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.TYPE)
	public static @interface Combinable {};
}
