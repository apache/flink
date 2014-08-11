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

import org.apache.commons.math.util.MathUtils;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.state.SlidingWindowState;

public class BatchReduceInvokable<IN, OUT> extends StreamReduceInvokable<IN, OUT> {
	private static final long serialVersionUID = 1L;
	private long batchSize;
	int counter = 0;

	public BatchReduceInvokable(GroupReduceFunction<IN, OUT> reduceFunction, long batchSize,
			long slideSize) {
		super(reduceFunction);
		this.reducer = reduceFunction;
		this.batchSize = batchSize;
		this.slideSize = slideSize;
		this.granularity = MathUtils.gcd(batchSize, slideSize);
		this.listSize = (int) granularity;
	}
	
	protected BatchReduceInvokable(ReduceFunction<IN> reduceFunction, long batchSize,
			long slideSize) {
		super(reduceFunction);
		this.batchSize = batchSize;
		this.slideSize = slideSize;
		this.granularity = MathUtils.gcd(batchSize, slideSize);
		this.listSize = (int) granularity;
	}

	@Override
	protected void mutableInvoke() throws Exception {
		throw new RuntimeException("Reducing mutable sliding batch is not supported.");
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		this.state = new SlidingWindowState<IN>(batchSize, slideSize, granularity);
	}

	@Override
	protected boolean batchNotFull() {
		counter++;
		if (counter < granularity) {
			return true;
		} else {
			counter = 0;
			return false;
		}
	}


}