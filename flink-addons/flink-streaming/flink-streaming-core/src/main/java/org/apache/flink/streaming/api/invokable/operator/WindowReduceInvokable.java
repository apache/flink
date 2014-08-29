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

package org.apache.flink.streaming.api.invokable.operator;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.configuration.Configuration;

public class WindowReduceInvokable<IN, OUT> extends BatchReduceInvokable<IN, OUT> {
	private static final long serialVersionUID = 1L;
	private long startTime;

	public WindowReduceInvokable(GroupReduceFunction<IN, OUT> reduceFunction, long windowSize,
			long slideInterval) {
		super(reduceFunction, windowSize,slideInterval);
	}

	@Override
	protected boolean batchNotFull() {
		long time = System.currentTimeMillis();
		if (time - startTime < granularity) {
			return true;
		} else {
			startTime = time;
			return false;
		}
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		startTime = System.currentTimeMillis();
	}

	protected void mutableInvoke() throws Exception {
		throw new RuntimeException("Reducing mutable sliding window is not supported.");
	}

}