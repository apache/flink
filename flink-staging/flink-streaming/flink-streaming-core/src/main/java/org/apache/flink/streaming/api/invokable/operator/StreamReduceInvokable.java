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
import org.apache.flink.streaming.api.invokable.ChainableInvokable;

public class StreamReduceInvokable<IN> extends ChainableInvokable<IN, IN> {
	private static final long serialVersionUID = 1L;

	protected ReduceFunction<IN> reducer;
	private IN currentValue;

	public StreamReduceInvokable(ReduceFunction<IN> reducer) {
		super(reducer);
		this.reducer = reducer;
		currentValue = null;
	}

	@Override
	public void invoke() throws Exception {
		while (isRunning && readNext() != null) {
			callUserFunctionAndLogException();
		}
	}

	@Override
	protected void callUserFunction() throws Exception {

		if (currentValue != null) {
			currentValue = reducer.reduce(copy(currentValue), nextObject);
		} else {
			currentValue = nextObject;

		}
		collector.collect(currentValue);

	}

	@Override
	public void collect(IN record) {
		if (isRunning) {
			nextObject = copyInput(record);
			callUserFunctionAndLogException();
		}
	}

}
