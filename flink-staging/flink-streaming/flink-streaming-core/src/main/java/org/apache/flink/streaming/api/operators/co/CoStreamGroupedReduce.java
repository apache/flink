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
	@SuppressWarnings("unchecked")
	public void handleStream1() throws Exception {
		CoReduceFunction<IN1, IN2, OUT> coReducer = (CoReduceFunction<IN1, IN2, OUT>) userFunction;
		Object key = reuse1.getKey(keySelector1);
		currentValue1 = values1.get(key);
		nextValue1 = reuse1.getObject();
		if (currentValue1 != null) {
			callUserFunctionAndLogException1();
			values1.put(key, reduced1);
			collector.collect(coReducer.map1(reduced1));
		} else {
			values1.put(key, nextValue1);
			collector.collect(coReducer.map1(nextValue1));
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public void handleStream2() throws Exception {
		CoReduceFunction<IN1, IN2, OUT> coReducer = (CoReduceFunction<IN1, IN2, OUT>) userFunction;
		Object key = reuse2.getKey(keySelector2);
		currentValue2 = values2.get(key);
		nextValue2 = reuse2.getObject();
		if (currentValue2 != null) {
			callUserFunctionAndLogException2();
			values2.put(key, reduced2);
			collector.collect(coReducer.map2(reduced2));
		} else {
			values2.put(key, nextValue2);
			collector.collect(coReducer.map2(nextValue2));
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void callUserFunction1() throws Exception {
		reduced1 = ((CoReduceFunction<IN1, IN2, OUT>) userFunction).reduce1(currentValue1, nextValue1);

	}

	@Override
	@SuppressWarnings("unchecked")
	protected void callUserFunction2() throws Exception {
		reduced2 = ((CoReduceFunction<IN1, IN2, OUT>) userFunction).reduce2(currentValue2, nextValue2);

	}

}
