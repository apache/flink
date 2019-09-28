/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.util;

import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * A test harness for testing a {@link TwoInputStreamOperator}.
 *
 * <p>This mock task provides the operator with a basic runtime context and allows pushing elements
 * and watermarks into the operator. {@link java.util.Deque}s containing the emitted elements
 * and watermarks can be retrieved. you are free to modify these.
 */
public class TwoInputStreamOperatorTestHarness<IN1, IN2, OUT> extends AbstractStreamOperatorTestHarness<OUT> {

	private final TwoInputStreamOperator<IN1, IN2, OUT> twoInputOperator;

	public TwoInputStreamOperatorTestHarness(TwoInputStreamOperator<IN1, IN2, OUT> operator) throws Exception {
		this(operator, 1, 1, 0);
	}

	public TwoInputStreamOperatorTestHarness(
			TwoInputStreamOperator<IN1, IN2, OUT> operator,
			int maxParallelism,
			int numSubtasks,
			int subtaskIndex) throws Exception {
		super(operator, maxParallelism, numSubtasks, subtaskIndex);

		this.twoInputOperator = operator;
	}

	public void processElement1(StreamRecord<IN1> element) throws Exception {
		twoInputOperator.setKeyContextElement1(element);
		twoInputOperator.processElement1(element);
	}

	public void processElement2(StreamRecord<IN2> element) throws Exception {
		twoInputOperator.setKeyContextElement2(element);
		twoInputOperator.processElement2(element);
	}

	public void processWatermark1(Watermark mark) throws Exception {
		twoInputOperator.processWatermark1(mark);
	}

	public void processWatermark2(Watermark mark) throws Exception {
		twoInputOperator.processWatermark2(mark);
	}

	public void processBothWatermarks(Watermark mark) throws Exception {
		twoInputOperator.processWatermark1(mark);
		twoInputOperator.processWatermark2(mark);
	}
}
