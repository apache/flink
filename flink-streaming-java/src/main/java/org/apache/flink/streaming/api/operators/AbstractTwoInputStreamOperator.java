/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * Base class for implementations of {@link TwoInputStreamOperator}.
 *
 * @param <IN1> The first input type of the operator
 * @param <IN2> The second input type of the operator
 * @param <OUT> The output type of the operator
 */
@Internal
public abstract class AbstractTwoInputStreamOperator<IN1, IN2, OUT>
		extends AbstractStreamOperator<OUT>
		implements TwoInputStreamOperator<IN1, IN2, OUT> {

	private static final long serialVersionUID = 1L;

	// We keep track of watermarks from both inputs, the combined input is the minimum
	// Once the minimum advances we emit a new watermark for downstream operators
	private long combinedWatermark = Long.MIN_VALUE;
	private long input1Watermark = Long.MIN_VALUE;
	private long input2Watermark = Long.MIN_VALUE;

	public AbstractTwoInputStreamOperator() {
	}

	public AbstractTwoInputStreamOperator(Function userFunction) {
		super(userFunction);
	}

	@Override
	public void processWatermark1(Watermark mark) throws Exception {
		input1Watermark = mark.getTimestamp();
		long newMin = Math.min(input1Watermark, input2Watermark);
		if (newMin > combinedWatermark) {
			combinedWatermark = newMin;
			output.emitWatermark(new Watermark(combinedWatermark));
		}
	}

	@Override
	public void processWatermark2(Watermark mark) throws Exception {
		input2Watermark = mark.getTimestamp();
		long newMin = Math.min(input1Watermark, input2Watermark);
		if (newMin > combinedWatermark) {
			combinedWatermark = newMin;
			output.emitWatermark(new Watermark(combinedWatermark));
		}
	}
}
