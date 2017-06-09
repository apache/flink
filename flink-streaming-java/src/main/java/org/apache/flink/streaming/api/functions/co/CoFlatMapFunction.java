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

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.util.Collector;

import java.io.Serializable;

/**
 * A CoFlatMapFunction implements a flat-map transformation over two
 * connected streams.
 *
 * <p>The same instance of the transformation function is used to transform
 * both of the connected streams. That way, the stream transformations can
 * share state.
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
@Public
public interface CoFlatMapFunction<IN1, IN2, OUT> extends Function, Serializable {

	/**
	 * This method is called for each element in the first of the connected streams.
	 *
	 * @param value The stream element
	 * @param out The collector to emit resulting elements to
	 * @throws Exception The function may throw exceptions which cause the streaming program
	 *                   to fail and go into recovery.
	 */
	void flatMap1(IN1 value, Collector<OUT> out) throws Exception;

	/**
	 * This method is called for each element in the second of the connected streams.
	 *
	 * @param value The stream element
	 * @param out The collector to emit resulting elements to
	 * @throws Exception The function may throw exceptions which cause the streaming program
	 *                   to fail and go into recovery.
	 */
	void flatMap2(IN2 value, Collector<OUT> out) throws Exception;
}
