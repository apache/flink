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

package org.apache.flink.streaming.api.invokable.operator;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.invokable.UserTaskInvokable;
import org.apache.flink.streaming.state.MutableTableState;

public class GroupReduceInvokable<IN> extends UserTaskInvokable<IN, IN> {
	private static final long serialVersionUID = 1L;

	private ReduceFunction<IN> reducer;
	private int keyPosition;
	private MutableTableState<Object, IN> values;

	public GroupReduceInvokable(ReduceFunction<IN> reducer, int keyPosition) {
		super(reducer);
		this.reducer = reducer;
		this.keyPosition = keyPosition;
		values = new MutableTableState<Object, IN>();
	}

	@Override
	protected void immutableInvoke() throws Exception {
		while ((reuse = recordIterator.next(reuse)) != null) {
			reduce();
			resetReuse();
		}
	}

	@Override
	protected void mutableInvoke() throws Exception {
		while ((reuse = recordIterator.next(reuse)) != null) {
			reduce();
		}
	}

	private IN reduced;
	private IN nextValue;
	private IN currentValue;
	
	private void reduce() throws Exception {
		Object key = reuse.getField(keyPosition);
		currentValue = values.get(key);
		nextValue = reuse.getObject();
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
