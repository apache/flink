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

package org.apache.flink.streaming.api.functions.co;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.util.Collector;

import java.io.Serializable;

/**
 * A {@code TimelyCoFlatMapFunction} implements a flat-map transformation over two
 * connected streams.
 * 
 * <p>The same instance of the transformation function is used to transform
 * both of the connected streams. That way, the stream transformations can
 * share state.
 *
 * <p>A {@code TimelyCoFlatMapFunction} can, in addition to the functionality of a normal
 * {@link CoFlatMapFunction}, also set timers and react to them firing.
 * 
 * <p>An example for the use of connected streams would be to apply rules that change over time
 * onto elements of a stream. One of the connected streams has the rules, the other stream the
 * elements to apply the rules to. The operation on the connected stream maintains the 
 * current set of rules in the state. It may receive either a rule update (from the first stream)
 * and update the state, or a data element (from the second stream) and apply the rules in the
 * state to the element. The result of applying the rules would be emitted.
 *
 * @param <IN1> Type of the first input.
 * @param <IN2> Type of the second input.
 * @param <OUT> Output type.
 */
@PublicEvolving
public interface TimelyCoFlatMapFunction<IN1, IN2, OUT> extends Function, Serializable {

	/**
	 * This method is called for each element in the first of the connected streams.
	 * 
	 * @param value The stream element
	 * @param timerService A {@link TimerService} that allows setting timers and querying the
	 *                        current time.
	 * @param out The collector to emit resulting elements to
	 * @throws Exception The function may throw exceptions which cause the streaming program
	 *                   to fail and go into recovery.
	 */
	void flatMap1(IN1 value, TimerService timerService, Collector<OUT> out) throws Exception;

	/**
	 * This method is called for each element in the second of the connected streams.
	 * 
	 * @param value The stream element
	 * @param timerService A {@link TimerService} that allows setting timers and querying the
	 *                        current time.
	 * @param out The collector to emit resulting elements to
	 * @throws Exception The function may throw exceptions which cause the streaming program
	 *                   to fail and go into recovery.
	 */
	void flatMap2(IN2 value, TimerService timerService, Collector<OUT> out) throws Exception;

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
	void onTimer(long timestamp, TimeDomain timeDomain, TimerService timerService, Collector<OUT> out) throws Exception ;

}
