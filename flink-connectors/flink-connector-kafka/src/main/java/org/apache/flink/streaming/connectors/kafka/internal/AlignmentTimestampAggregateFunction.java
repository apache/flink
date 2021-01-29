/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.internal;

import org.apache.flink.api.common.functions.AggregateFunction;

import javafx.util.Pair;

import java.util.HashMap;
import java.util.Map;

/**
 * Used to compute the global minimum of current event time across all kafka partitions.
 * <p/>Maintains a Map of subtaskId to subtask minimum. Computes the minimum
 * of all subtask in the getResult function.
 */
public class AlignmentTimestampAggregateFunction
	implements AggregateFunction<Pair<Integer, Long>, Map<Integer, Long>, Long> {

	@Override
	public Map<Integer, Long> createAccumulator() {
		return new HashMap<>();
	}

	@Override
	public Map<Integer, Long> add(Pair<Integer, Long> subtaskTs, Map<Integer, Long> accumulator) {
		accumulator.put(subtaskTs.getKey(), subtaskTs.getValue());
		return accumulator;
	}

	@Override
	public Long getResult(Map<Integer, Long> accumulator) {
		return accumulator.values().stream().min(Long::compareTo).get();
	}

	@Override
	public Map<Integer, Long> merge(Map<Integer, Long> a, Map<Integer, Long> b) {
		// not required
		throw new UnsupportedOperationException();
	}

}
