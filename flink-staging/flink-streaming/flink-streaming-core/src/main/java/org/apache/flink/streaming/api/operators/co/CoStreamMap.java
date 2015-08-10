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

package org.apache.flink.streaming.api.operators.co;

import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public class CoStreamMap<IN1, IN2, OUT>
		extends AbstractUdfStreamOperator<OUT, CoMapFunction<IN1, IN2, OUT>>
		implements TwoInputStreamOperator<IN1, IN2, OUT> {

	private static final long serialVersionUID = 1L;

	// We keep track of watermarks from both inputs, the combined input is the minimum
	// Once the minimum advances we emit a new watermark for downstream operators
	private long combinedWatermark = Long.MIN_VALUE;
	private long input1Watermark = Long.MAX_VALUE;
	private long input2Watermark = Long.MAX_VALUE;

	public CoStreamMap(CoMapFunction<IN1, IN2, OUT> mapper) {
		super(mapper);
	}

	@Override
	public void processElement1(StreamRecord<IN1> element) throws Exception {
		output.collect(element.replace(userFunction.map1(element.getValue())));
	}

	@Override
	public void processElement2(StreamRecord<IN2> element) throws Exception {
		output.collect(element.replace(userFunction.map2(element.getValue())));
	}

	@Override
	public void processWatermark1(Watermark mark) throws Exception {
		input1Watermark = mark.getTimestamp();
		long newMin = Math.min(input1Watermark, input2Watermark);
		if (newMin > combinedWatermark && input1Watermark != Long.MAX_VALUE && input2Watermark != Long.MAX_VALUE) {
			combinedWatermark = newMin;
			output.emitWatermark(new Watermark(combinedWatermark));
		}
	}

	@Override
	public void processWatermark2(Watermark mark) throws Exception {
		input2Watermark = mark.getTimestamp();
		long newMin = Math.min(input1Watermark, input2Watermark);
		if (newMin > combinedWatermark && input1Watermark != Long.MAX_VALUE && input2Watermark != Long.MAX_VALUE) {
			combinedWatermark = newMin;
			output.emitWatermark(new Watermark(combinedWatermark));
		}
	}
}
