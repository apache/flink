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

package org.apache.flink.streaming.api.operators;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;

public class StreamGroupedReduce<IN> extends StreamReduce<IN> {

	private static final long serialVersionUID = 1L;

	private KeySelector<IN, ?> keySelector;
	private Map<Object, IN> values;

	public StreamGroupedReduce(ReduceFunction<IN> reducer, KeySelector<IN, ?> keySelector) {
		super(reducer);
		this.keySelector = keySelector;
		values = new HashMap<Object, IN>();
	}

	@Override
	public void processElement(IN element) throws Exception {
		Object key = keySelector.getKey(element);
		IN currentValue = values.get(key);
		if (currentValue != null) {
			// TODO: find a way to let operators copy elements (maybe)
			IN reduced = userFunction.reduce(currentValue, element);
			values.put(key, reduced);
			output.collect(reduced);
		} else {
			values.put(key, element);
			output.collect(element);
		}
	}

}
