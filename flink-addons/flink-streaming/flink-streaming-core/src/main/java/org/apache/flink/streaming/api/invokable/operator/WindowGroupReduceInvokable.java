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

package org.apache.flink.streaming.api.invokable.operator;

import java.util.LinkedList;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;

public class WindowGroupReduceInvokable<IN, OUT> extends WindowInvokable<IN, OUT> {

	private static final long serialVersionUID = 1L;
	GroupReduceFunction<IN, OUT> reducer;

	public WindowGroupReduceInvokable(GroupReduceFunction<IN, OUT> userFunction,
			LinkedList<TriggerPolicy<IN>> triggerPolicies,
			LinkedList<EvictionPolicy<IN>> evictionPolicies) {
		super(userFunction, triggerPolicies, evictionPolicies);
		this.reducer = userFunction;
	}

	@Override
	protected void callUserFunction() throws Exception {
		reducer.reduce(copyBuffer(), collector);
	}

	public LinkedList<IN> copyBuffer() {
		LinkedList<IN> copy = new LinkedList<IN>();
		for (IN element : buffer) {
			copy.add(copy(element));
		}
		return copy;
	}

}
