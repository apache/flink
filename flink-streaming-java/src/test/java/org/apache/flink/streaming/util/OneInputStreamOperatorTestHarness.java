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
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A test harness for testing a {@link OneInputStreamOperator}.
 *
 * <p>This mock task provides the operator with a basic runtime context and allows pushing elements
 * and watermarks into the operator. {@link java.util.Deque}s containing the emitted elements
 * and watermarks can be retrieved. You are free to modify these.
 */
public class OneInputStreamOperatorTestHarness<IN, OUT>
		extends AbstractStreamOperatorTestHarness<OUT> {

	/** Empty if the {@link #operator} is not {@link MultipleInputStreamOperator}. */
	private final List<Input> inputs = new ArrayList<>();

	private long currentWatermark;

	public OneInputStreamOperatorTestHarness(
		OneInputStreamOperator<IN, OUT> operator,
		StreamOperatorFactory<OUT> factory,
		@Nullable TypeSerializer<IN> typeSerializerIn,
		MockEnvironment environment,
		boolean isInternalEnvironment,
		OperatorID operatorID) throws Exception {

		super(operator, factory, environment, isInternalEnvironment, operatorID);

		// some unit test might not set typeSerializerIn.
		if (typeSerializerIn != null) {
			config.setTypeSerializersIn(typeSerializerIn);
		}
	}

	@Override
	public void setup(TypeSerializer<OUT> outputSerializer) {
		super.setup(outputSerializer);
		if (operator instanceof MultipleInputStreamOperator) {
			checkState(inputs.isEmpty());
			inputs.addAll(((MultipleInputStreamOperator) operator).getInputs());
		}
	}

	public OneInputStreamOperator<IN, OUT> getOneInputOperator() {
		return (OneInputStreamOperator<IN, OUT>) this.operator;
	}

	public void processElement(IN value, long timestamp) throws Exception {
		processElement(new StreamRecord<>(value, timestamp));
	}

	public void processElement(StreamRecord<IN> element) throws Exception {
		if (inputs.isEmpty()) {
			operator.setKeyContextElement1(element);
			getOneInputOperator().processElement(element);
		}
		else {
			checkState(inputs.size() == 1);
			Input input = inputs.get(0);
			input.setKeyContextElement(element);
			input.processElement(element);
		}
	}

	public void processElements(Collection<StreamRecord<IN>> elements) throws Exception {
		for (StreamRecord<IN> element: elements) {
			processElement(element);
		}
	}

	public void processWatermark(long watermark) throws Exception {
		processWatermark(new Watermark(watermark));
	}

	public void processWatermark(Watermark mark) throws Exception {
		currentWatermark = mark.getTimestamp();
		if (inputs.isEmpty()) {
			getOneInputOperator().processWatermark(mark);
		}
		else {
			checkState(inputs.size() == 1);
			Input input = inputs.get(0);
			input.processWatermark(mark);
		}
	}

	public void endInput() throws Exception {
		if (operator instanceof BoundedOneInput) {
			((BoundedOneInput) operator).endInput();
		}
	}

	public long getCurrentWatermark() {
		return currentWatermark;
	}

}
