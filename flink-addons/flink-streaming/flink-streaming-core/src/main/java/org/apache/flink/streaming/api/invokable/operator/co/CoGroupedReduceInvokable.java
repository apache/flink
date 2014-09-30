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

package org.apache.flink.streaming.api.invokable.operator.co;

import org.apache.flink.streaming.api.function.co.CoReduceFunction;
import org.apache.flink.streaming.state.MutableTableState;

public class CoGroupedReduceInvokable<IN1, IN2, OUT> extends CoReduceInvokable<IN1, IN2, OUT> {
	private static final long serialVersionUID = 1L;

	private int keyPosition1;
	private int keyPosition2;
	private MutableTableState<Object, IN1> values1;
	private MutableTableState<Object, IN2> values2;
	IN1 reduced1;
	IN2 reduced2;

	public CoGroupedReduceInvokable(CoReduceFunction<IN1, IN2, OUT> coReducer, int keyPosition1,
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
	public void handleStream2() throws Exception {
		Object key = reuse2.getField(keyPosition2);
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
	protected void callUserFunction1() throws Exception {
		reduced1 = coReducer.reduce1(currentValue1, nextValue1);

	}

	@Override
	protected void callUserFunction2() throws Exception {
		reduced2 = coReducer.reduce2(currentValue2, nextValue2);

	}

}
