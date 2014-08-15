/**
 *
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
 *
 */

package org.apache.flink.streaming.api.invokable.operator.co;

import org.apache.flink.streaming.api.function.co.CoReduceFunction;
import org.apache.flink.streaming.state.MutableTableState;

public class CoGroupReduceInvokable<IN1, IN2, OUT> extends CoInvokable<IN1, IN2, OUT> {
	private static final long serialVersionUID = 1L;

	private CoReduceFunction<IN1, IN2, OUT> coReducer;
	private int keyPosition1;
	private MutableTableState<Object, IN1> values1;

	private int keyPosition2;
	private MutableTableState<Object, IN2> values2;

	public CoGroupReduceInvokable(CoReduceFunction<IN1, IN2, OUT> coReducer, int keyPosition1,
			int keyPosition2) {
		super(coReducer);
		this.coReducer = coReducer;
		this.keyPosition1 = keyPosition1;
		this.keyPosition2 = keyPosition2;
		values1 = new MutableTableState<Object, IN1>();
		values2 = new MutableTableState<Object, IN2>();
	}

	@Override
	public void handleStream1() throws Exception {
		Object key = reuse1.getField(keyPosition1);
		IN1 currentValue = values1.get(key);
		IN1 nextValue = reuse1.getObject();
		if (currentValue != null) {
			IN1 reduced = coReducer.reduce1(currentValue, nextValue);
			values1.put(key, reduced);
			collector.collect(coReducer.map1(reduced));
		} else {
			values1.put(key, nextValue);
			collector.collect(coReducer.map1(nextValue));
		}
	}

	@Override
	public void handleStream2() throws Exception {
		Object key = reuse2.getField(keyPosition2);
		IN2 currentValue = values2.get(key);
		IN2 nextValue = reuse2.getObject();
		if (currentValue != null) {
			IN2 reduced = coReducer.reduce2(currentValue, nextValue);
			values2.put(key, reduced);
			collector.collect(coReducer.map2(reduced));
		} else {
			values2.put(key, nextValue);
			collector.collect(coReducer.map2(nextValue));
		}
	}

}
