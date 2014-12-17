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

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;

public class GroupedReduceInvokable<IN> extends StreamReduceInvokable<IN> {
	private static final long serialVersionUID = 1L;

	private KeySelector<IN, ?> keySelector;
	private Map<Object, IN> values;
	private IN reduced;

	public GroupedReduceInvokable(ReduceFunction<IN> reducer, KeySelector<IN, ?> keySelector) {
		super(reducer);
		this.keySelector = keySelector;
		values = new HashMap<Object, IN>();
	}

	@Override
	protected void reduce() throws Exception {
		Object key = nextRecord.getKey(keySelector);
		currentValue = values.get(key);
		nextValue = nextRecord.getObject();
		if (currentValue != null) {
			callUserFunctionAndLogException();
			values.put(key, reduced);
			collector.collect(reduced);
		} else {
			values.put(key, nextValue);
			collector.collect(nextValue);
		}
	}

	@Override
	protected void callUserFunction() throws Exception {
		reduced = reducer.reduce(currentValue, nextValue);
	}

}
