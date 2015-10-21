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

import java.io.Serializable;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.util.Collector;

/**
 * A CoFlatMapFunction represents a FlatMap transformation with two different
 * input types.
 *
 * @param <IN1>
 *            Type of the first input.
 * @param <IN2>
 *            Type of the second input.
 * @param <OUT>
 *            Output type.
 */
public interface CoFlatMapFunction<IN1, IN2, OUT> extends Function, Serializable {

	void flatMap1(IN1 value, Collector<OUT> out) throws Exception;

	void flatMap2(IN2 value, Collector<OUT> out) throws Exception;
}
