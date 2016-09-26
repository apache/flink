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

package org.apache.flink.streaming.api.functions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.util.Collector;

import java.io.Serializable;

/**
 * Base interface for timely flatMap functions. FlatMap functions take elements and transform them,
 * into zero, one, or more elements. Typical applications can be splitting elements, or unnesting lists
 * and arrays.
 *
 * <p>A {@code TimelyFlatMapFunction} can, in addition to the functionality of a normal
 * {@link org.apache.flink.api.common.functions.FlatMapFunction}, also set timers and react
 * to them firing.
 *
 * <pre>{@code
 * DataStream<X> input = ...;
 *
 * DataStream<Y> result = input.flatMap(new MyTimelyFlatMapFunction());
 * }</pre>
 *
 * @param <I> Type of the input elements.
 * @param <O> Type of the returned elements.
 */
@PublicEvolving
public interface TimelyFlatMapFunction<I, O> extends Function, Serializable {

	/**
	 * The core method of the {@code TimelyFlatMapFunction}. Takes an element from the input data set and transforms
	 * it into zero, one, or more elements.
	 *
	 * @param value The input value.
	 * @param timerService A {@link TimerService} that allows setting timers and querying the
	 *                        current time.
	 * @param out The collector for returning result values.
	 *
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
	 *                   to fail and may trigger recovery.
	 */
	void flatMap(I value, TimerService timerService, Collector<O> out) throws Exception;

	/**
	 * Called when a timer set using {@link TimerService} fires.
	 *
	 * @param timestamp The timestamp of the firing timer.
	 * @param timeDomain The {@link TimeDomain} of the firing timer.
	 * @param timerService A {@link TimerService} that allows setting timers and querying the
	 *                        current time.
	 * @param out The collector for returning result values.
	 *
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
	 *                   to fail and may trigger recovery.
	 */
	void onTimer(long timestamp, TimeDomain timeDomain, TimerService timerService, Collector<O> out) throws Exception ;

}
