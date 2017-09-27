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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

import java.util.Collection;

/**
 * A test harness for testing a {@link OneInputStreamOperator}.
 *
 * <p>This mock task provides the operator with a basic runtime context and allows pushing elements
 * and watermarks into the operator. {@link java.util.Deque}s containing the emitted elements
 * and watermarks can be retrieved. You are free to modify these.
 */
public class OneInputStreamOperatorTestHarness<IN, OUT>
		extends AbstractStreamOperatorTestHarness<OUT> {

	private final OneInputStreamOperator<IN, OUT> oneInputOperator;

	private long currentWatermark;

	public OneInputStreamOperatorTestHarness(
			OneInputStreamOperator<IN, OUT> operator,
			TypeSerializer<IN> typeSerializerIn) throws Exception {
		this(operator, 1, 1, 0);

		config.setTypeSerializerIn1(Preconditions.checkNotNull(typeSerializerIn));
	}

	public OneInputStreamOperatorTestHarness(
		OneInputStreamOperator<IN, OUT> operator,
		int maxParallelism,
		int parallelism,
		int subtaskIndex,
		TypeSerializer<IN> typeSerializerIn) throws Exception {
		this(operator, maxParallelism, parallelism, subtaskIndex);

		config.setTypeSerializerIn1(Preconditions.checkNotNull(typeSerializerIn));
	}

	public OneInputStreamOperatorTestHarness(
		OneInputStreamOperator<IN, OUT> operator,
		TypeSerializer<IN> typeSerializerIn,
		MockEnvironment environment) throws Exception {
		this(operator, environment);

		config.setTypeSerializerIn1(Preconditions.checkNotNull(typeSerializerIn));
	}

	public OneInputStreamOperatorTestHarness(OneInputStreamOperator<IN, OUT> operator) throws Exception {
		this(operator, 1, 1, 0);
	}

	public OneInputStreamOperatorTestHarness(
			OneInputStreamOperator<IN, OUT> operator,
			int maxParallelism,
			int parallelism,
			int subtaskIndex) throws Exception {
		super(operator, maxParallelism, parallelism, subtaskIndex);

		this.oneInputOperator = operator;
	}

	public OneInputStreamOperatorTestHarness(
		OneInputStreamOperator<IN, OUT> operator,
		MockEnvironment environment) throws Exception {
		super(operator, environment);

		this.oneInputOperator = operator;
	}

	public void processElement(IN value, long timestamp) throws Exception {
		processElement(new StreamRecord<>(value, timestamp));
	}

	public void processElement(StreamRecord<IN> element) throws Exception {
		operator.setKeyContextElement1(element);
		oneInputOperator.processElement(element);
	}

	public void processElements(Collection<StreamRecord<IN>> elements) throws Exception {
		for (StreamRecord<IN> element: elements) {
			operator.setKeyContextElement1(element);
			oneInputOperator.processElement(element);
		}
	}

	public void processWatermark(long watermark) throws Exception {
		processWatermark(new Watermark(watermark));
	}

	public void processWatermark(Watermark mark) throws Exception {
		currentWatermark = mark.getTimestamp();
		oneInputOperator.processWatermark(mark);
	}

	public long getCurrentWatermark() {
		return currentWatermark;
	}
}
