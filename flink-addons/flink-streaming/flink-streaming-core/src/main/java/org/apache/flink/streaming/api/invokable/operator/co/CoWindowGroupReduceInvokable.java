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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.function.co.CoGroupReduceFunction;
import org.apache.flink.streaming.api.invokable.util.TimeStamp;

public class CoWindowGroupReduceInvokable<IN1, IN2, OUT> extends CoGroupReduceInvokable<IN1, IN2, OUT> {
	private static final long serialVersionUID = 1L;

	protected long startTime1;
	protected long startTime2;
	protected long endTime1;
	protected long endTime2;
	protected long currentTime;
	protected TimeStamp<IN1> timestamp1;
	protected TimeStamp<IN2> timestamp2;

	public CoWindowGroupReduceInvokable(CoGroupReduceFunction<IN1, IN2, OUT> reduceFunction,
			long windowSize1, long windowSize2, long slideInterval1, long slideInterval2,
			TimeStamp<IN1> timestamp1, TimeStamp<IN2> timestamp2) {
		super(reduceFunction, windowSize1, windowSize2, slideInterval1, slideInterval2);
		this.timestamp1 = timestamp1;
		this.timestamp2 = timestamp2;
		startTime1 = timestamp1.getStartTime();
		startTime2 = timestamp2.getStartTime();
		endTime1 = startTime1 + windowSize1;
		endTime2 = startTime2 + windowSize2;
	}

	@Override
	protected boolean windowStart1() throws Exception {
		if (currentTime - startTime1 >= slideInterval1) {
			startTime1 += slideInterval1;
			return true;
		}
		return false;
	}

	@Override
	protected boolean windowStart2() throws Exception {
		if (currentTime - startTime2 >= slideInterval2) {
			startTime2 += slideInterval2;
			return true;
		}
		return false;
	}

	@Override
	protected boolean windowEnd1() throws Exception {
		if (currentTime >= endTime1) {
			endTime1 += slideInterval1;
			return true;
		}
		return false;
	}

	@Override
	protected boolean windowEnd2() throws Exception {
		if (currentTime >= endTime2) {
			endTime2 += slideInterval2;
			return true;
		}
		return false;
	}

	@Override
	protected void initialize1() {
		currentTime = timestamp1.getTimestamp(reuse1.getObject());
	}

	@Override
	protected void initialize2() {
		currentTime = timestamp2.getTimestamp(reuse2.getObject());
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
	}

}
