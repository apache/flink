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

public class CoReduceInvokable<IN1, IN2, OUT> extends CoInvokable<IN1, IN2, OUT> {
	private static final long serialVersionUID = 1L;

	protected CoReduceFunction<IN1, IN2, OUT> coReducer;
	protected IN1 currentValue1 = null;
	protected IN2 currentValue2 = null;
	protected IN1 nextValue1 = null;
	protected IN2 nextValue2 = null;

	public CoReduceInvokable(CoReduceFunction<IN1, IN2, OUT> coReducer) {
		super(coReducer);
		this.coReducer = coReducer;
		currentValue1 = null;
		currentValue2 = null;
	}

	@Override
	public void handleStream1() throws Exception {
		nextValue1 = reuse1.getObject();
		callUserFunctionAndLogException1();
	}

	@Override
	public void handleStream2() throws Exception {
		nextValue2 = reuse2.getObject();
		callUserFunctionAndLogException2();
	}

	@Override
	protected void callUserFunction1() throws Exception {
		if (currentValue1 != null) {
			currentValue1 = coReducer.reduce1(currentValue1, nextValue1);
		} else {
			currentValue1 = nextValue1;
		}
		collector.collect(coReducer.map1(currentValue1));
	}

	@Override
	protected void callUserFunction2() throws Exception {
		if (currentValue2 != null) {
			currentValue2 = coReducer.reduce2(currentValue2, nextValue2);
		} else {
			currentValue2 = nextValue2;
		}
		collector.collect(coReducer.map2(currentValue2));
	}

}
