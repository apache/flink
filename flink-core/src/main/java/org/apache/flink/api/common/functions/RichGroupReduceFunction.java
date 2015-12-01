/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.functions;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.apache.flink.annotation.Public;
import org.apache.flink.util.Collector;

/**
 * Rich variant of the {@link GroupReduceFunction}. As a {@link RichFunction}, it gives access to the
 * {@link org.apache.flink.api.common.functions.RuntimeContext} and provides setup and teardown methods:
 * {@link RichFunction#open(org.apache.flink.configuration.Configuration)} and
 * {@link RichFunction#close()}.
 * 
 * @param <IN> Type of the elements that this function processes.
 * @param <OUT> The type of the elements returned by the user-defined function.
 */
@Public
public abstract class RichGroupReduceFunction<IN, OUT> extends AbstractRichFunction implements GroupReduceFunction<IN, OUT>, GroupCombineFunction<IN, IN> {
	
	private static final long serialVersionUID = 1L;

	@Override
	public abstract void reduce(Iterable<IN> values, Collector<OUT> out) throws Exception;
	
	/**
	 * The combine methods pre-reduces elements. It may be called on subsets of the data
	 * before the actual reduce function. This is often helpful to lower data volume prior
	 * to reorganizing the data in an expensive way, as might be required for the final
	 * reduce function.
	 * <p>
	 * This method is only ever invoked when the subclass of {@link RichGroupReduceFunction}
	 * adds the {@link Combinable} annotation, or if the <i>combinable</i> flag is set when defining
	 * the <i>reduceGroup</i> operation via
	 * org.apache.flink.api.java.operators.GroupReduceOperator#setCombinable(boolean).
	 * <p>
	 * Since the reduce function will be called on the result of this method, it is important that this
	 * method returns the same data type as it consumes. By default, this method only calls the
	 * {@link #reduce(Iterable, Collector)} method. If the behavior in the pre-reducing is different
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
	public void combine(Iterable<IN> values, Collector<IN> out) throws Exception {
		@SuppressWarnings("unchecked")
		Collector<OUT> c = (Collector<OUT>) out;
		reduce(values, c);
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * This annotation can be added to classes that extend {@link RichGroupReduceFunction}, in oder to mark
	 * them as "combinable". The system may call the {@link RichGroupReduceFunction#combine(Iterable, Collector)}
	 * method on such functions, to pre-reduce the data before transferring it over the network to
	 * the actual group reduce operation.
	 * <p>
	 * Marking combinable functions as such is in general beneficial for performance.
	 */
	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.TYPE)
	@Public
	public static @interface Combinable {}
}
