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

package org.apache.flink.table.runtime.operators.multipleinput.input;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.operators.AbstractInput;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.operators.multipleinput.MultipleInputTestBase;
import org.apache.flink.table.runtime.operators.multipleinput.TestingKeySelector;
import org.apache.flink.table.runtime.operators.multipleinput.TestingOneInputStreamOperator;
import org.apache.flink.table.runtime.operators.multipleinput.TestingTwoInputStreamOperator;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Test for the sub-classes of {@link Input}.
 */
public class InputTest extends MultipleInputTestBase {

	private StreamRecord<RowData> element;
	private Watermark watermark;
	private LatencyMarker latencyMarker;
	private TestingMultipleInputOperator owner;

	@Before
	public void setup() throws Exception {
		element = new StreamRecord<>(GenericRowData.of(StringData.fromString("123")), 456);
		watermark = new Watermark(1223456789);
		latencyMarker = new LatencyMarker(122345678, new OperatorID(123, 456), 1);
		owner = new TestingMultipleInputOperator(createStreamOperatorParameters());
	}

	@Test
	public void testOneInput() throws Exception {
		TestingOneInputStreamOperator op = createOneInputStreamOperator();
		OneInput input = new OneInput(owner, 1, op);

		input.processElement(element);
		assertEquals(element, op.getCurrentElement());

		input.processWatermark(watermark);
		assertEquals(watermark, op.getCurrentWatermark());

		input.processLatencyMarker(latencyMarker);
		assertEquals(latencyMarker, op.getCurrentLatencyMarker());
	}

	@Test
	public void testOneInputWithKeySelector() throws Exception {
		owner.getOperatorConfig().setStatePartitioner(0, new TestingKeySelector());

		TestingOneInputStreamOperator op = createOneInputStreamOperator();
		OneInput input = new OneInput(owner, 1, op);

		assertNull(op.getCurrentStateKey());
		input.setKeyContextElement(element);
		assertEquals(element.getValue(), op.getCurrentStateKey());
	}

	@Test
	public void testFirstInputOfTwoInput() throws Exception {
		TestingTwoInputStreamOperator op = createTwoInputStreamOperator();
		FirstInputOfTwoInput input = new FirstInputOfTwoInput(owner, 1, op);

		input.processElement(element);
		assertEquals(element, op.getCurrentElement1());
		assertNull(op.getCurrentElement2());

		input.processWatermark(watermark);
		assertEquals(watermark, op.getCurrentWatermark1());
		assertNull(op.getCurrentWatermark2());

		input.processLatencyMarker(latencyMarker);
		assertEquals(latencyMarker, op.getCurrentLatencyMarker1());
		assertNull(op.getCurrentLatencyMarker2());
	}

	@Test
	public void testFirstInputOfTwoInputWithKeySelector() throws Exception {
		owner.getOperatorConfig().setStatePartitioner(0, new TestingKeySelector());

		TestingTwoInputStreamOperator op = createTwoInputStreamOperator();
		FirstInputOfTwoInput input = new FirstInputOfTwoInput(owner, 1, op);

		assertNull(op.getCurrentStateKey());
		input.setKeyContextElement(element);
		assertEquals(element.getValue(), op.getCurrentStateKey());
	}

	@Test
	public void testSecondInputOfTwoInput() throws Exception {
		TestingTwoInputStreamOperator op = createTwoInputStreamOperator();
		SecondInputOfTwoInput input = new SecondInputOfTwoInput(owner, 2, op);

		input.processElement(element);
		assertEquals(element, op.getCurrentElement2());
		assertNull(op.getCurrentElement1());

		input.processWatermark(watermark);
		assertEquals(watermark, op.getCurrentWatermark2());
		assertNull(op.getCurrentWatermark1());

		input.processLatencyMarker(latencyMarker);
		assertEquals(latencyMarker, op.getCurrentLatencyMarker2());
		assertNull(op.getCurrentLatencyMarker1());
	}

	@Test
	public void testSecondInputOfTwoInputWithKeySelector() throws Exception {
		owner.getOperatorConfig().setStatePartitioner(1, new TestingKeySelector());

		TestingTwoInputStreamOperator op = createTwoInputStreamOperator();
		SecondInputOfTwoInput input = new SecondInputOfTwoInput(owner, 2, op);

		assertNull(op.getCurrentStateKey());
		input.setKeyContextElement(element);
		assertEquals(element.getValue(), op.getCurrentStateKey());
	}

	private static class TestingMultipleInputOperator extends AbstractStreamOperatorV2<RowData>
			implements MultipleInputStreamOperator<RowData>, BoundedMultiInput {

		private static final long serialVersionUID = 1L;

		public TestingMultipleInputOperator(StreamOperatorParameters<RowData> parameters) {
			super(parameters, 3);
		}

		@Override
		public List<Input> getInputs() {
			return Arrays.asList(
					new TestInput(this, 1),
					new TestInput(this, 2),
					new TestInput(this, 3)
			);
		}

		@Override
		public void endInput(int inputId) {
		}

		@Override
		public void close() throws Exception {
			super.close();
		}

		static class TestInput extends AbstractInput<RowData, RowData> {
			public TestInput(AbstractStreamOperatorV2<RowData> owner, int inputId) {
				super(owner, inputId);
			}

			@Override
			public void processElement(StreamRecord<RowData> element) throws Exception {
				output.collect(element);
			}
		}
	}
}
