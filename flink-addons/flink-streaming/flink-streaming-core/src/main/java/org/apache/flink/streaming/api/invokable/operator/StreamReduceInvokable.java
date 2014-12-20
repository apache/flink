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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.invokable.StreamInvokable;

public class StreamReduceInvokable<IN> extends StreamInvokable<IN, IN> {
	private static final long serialVersionUID = 1L;

	protected ReduceFunction<IN> reducer;
	protected IN currentValue;
	protected IN nextValue;

	public StreamReduceInvokable(ReduceFunction<IN> reducer) {
		super(reducer);
		this.reducer = reducer;
		currentValue = null;
	}

	@Override
	public void invoke() throws Exception {
		while (readNext() != null) {
			reduce();
		}
	}

	protected void reduce() throws Exception {
		nextValue = nextRecord.getObject();
		callUserFunctionAndLogException();

	}

	@Override
	protected void callUserFunction() throws Exception {
		if (currentValue != null) {
			currentValue = reducer.reduce(currentValue, nextValue);
		} else {
			currentValue = nextValue;

		}
		collector.collect(currentValue);

	}
}
