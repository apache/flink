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

package org.apache.flink.streaming.api.operators.co;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.co.CoReduceFunction;

public class CoStreamGroupedReduce<IN1, IN2, OUT> extends CoStreamReduce<IN1, IN2, OUT> {

	private static final long serialVersionUID = 1L;

	protected KeySelector<IN1, ?> keySelector1;
	protected KeySelector<IN2, ?> keySelector2;
	private Map<Object, IN1> values1;
	private Map<Object, IN2> values2;
	IN1 reduced1;
	IN2 reduced2;

	public CoStreamGroupedReduce(CoReduceFunction<IN1, IN2, OUT> coReducer,
			KeySelector<IN1, ?> keySelector1, KeySelector<IN2, ?> keySelector2) {
		super(coReducer);
		this.keySelector1 = keySelector1;
		this.keySelector2 = keySelector2;
		values1 = new HashMap<Object, IN1>();
		values2 = new HashMap<Object, IN2>();
	}

	@Override
	public void processElement1(IN1 element) throws Exception {
		Object key = keySelector1.getKey(element);
		currentValue1 = values1.get(key);
		if (currentValue1 != null) {
			reduced1 = userFunction.reduce1(currentValue1, element);
			values1.put(key, reduced1);
			output.collect(userFunction.map1(reduced1));
		} else {
			values1.put(key, element);
			output.collect(userFunction.map1(element));
		}
	}

	@Override
	public void processElement2(IN2 element) throws Exception {
		Object key = keySelector2.getKey(element);
		currentValue2 = values2.get(key);
		if (currentValue2 != null) {
			reduced2 = userFunction.reduce2(currentValue2, element);
			values2.put(key, reduced2);
			output.collect(userFunction.map2(reduced2));
		} else {
			values2.put(key, element);
			output.collect(userFunction.map2(element));
		}
	}
}
