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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.util.Collector;

public class JoinWindowFunction<IN1, IN2, OUT> implements CoWindowFunction<IN1, IN2, OUT> {
	private static final long serialVersionUID = 1L;

	private KeySelector<IN1, ?> keySelector1;
	private KeySelector<IN2, ?> keySelector2;
	private JoinFunction<IN1, IN2, OUT> joinFunction;

	public JoinWindowFunction(KeySelector<IN1, ?> keySelector1, KeySelector<IN2, ?> keySelector2,
			JoinFunction<IN1, IN2, OUT> joinFunction) {
		this.keySelector1 = keySelector1;
		this.keySelector2 = keySelector2;
		this.joinFunction = joinFunction;
	}

	@Override
	public void coWindow(List<IN1> first, List<IN2> second, Collector<OUT> out) throws Exception {

		Map<Object, List<IN1>> map = build(first);

		for (IN2 record : second) {
			Object key = keySelector2.getKey(record);
			List<IN1> match = map.get(key);
			if (match != null) {
				for (IN1 matching : match) {
					out.collect(joinFunction.join(matching, record));
				}
			}
		}

	}

	private Map<Object, List<IN1>> build(List<IN1> records) throws Exception {

		Map<Object, List<IN1>> map = new HashMap<Object, List<IN1>>();

		for (IN1 record : records) {
			Object key = keySelector1.getKey(record);
			List<IN1> current = map.get(key);
			if (current == null) {
				current = new LinkedList<IN1>();
				map.put(key, current);
			}
			current.add(record);
		}

		return map;
	}
}