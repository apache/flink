/**
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

	private CoReduceFunction<IN1, IN2, OUT> coReducer;
	private IN1 currentValue1 = null;
	private IN2 currentValue2 = null;

	public CoReduceInvokable(CoReduceFunction<IN1, IN2, OUT> coReducer) {
		super(coReducer);
		this.coReducer = coReducer;
	}

	@Override
	public void handleStream1() throws Exception {
		IN1 nextValue = reuse1.getObject();
		if (currentValue1 != null) {
			currentValue1 = coReducer.reduce1(currentValue1, nextValue);
			collector.collect(coReducer.map1(currentValue1));
		} else {
			currentValue1 = nextValue;
			collector.collect(coReducer.map1(nextValue));
		}
	}

	@Override
	public void handleStream2() throws Exception {
		IN2 nextValue = reuse2.getObject();
		if (currentValue2 != null) {
			currentValue2 = coReducer.reduce2(currentValue2, nextValue);
			collector.collect(coReducer.map2(currentValue2));
		} else {
			currentValue2 = nextValue;
			collector.collect(coReducer.map2(nextValue));
		}
	}

}
