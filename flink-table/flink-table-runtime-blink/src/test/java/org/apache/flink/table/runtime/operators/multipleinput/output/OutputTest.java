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

package org.apache.flink.table.runtime.operators.multipleinput.output;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.operators.multipleinput.MultipleInputTestBase;
import org.apache.flink.table.runtime.operators.multipleinput.TestingOneInputStreamOperator;
import org.apache.flink.table.runtime.operators.multipleinput.TestingTwoInputStreamOperator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

/**
 * Test for the sub-classes of {@link Output}.
 */
public class OutputTest extends MultipleInputTestBase {

	private StreamRecord<RowData> element;
	private Watermark watermark;
	private LatencyMarker latencyMarker;
	private TypeSerializer<RowData> serializer;

	@Before
	public void setup() {
		element = new StreamRecord<>(GenericRowData.of(StringData.fromString("123")), 456);
		watermark = new Watermark(1223456789);
		latencyMarker = new LatencyMarker(122345678, new OperatorID(123, 456), 1);
		serializer = InternalTypeInfo.of(RowType.of(DataTypes.STRING().getLogicalType()))
				.createSerializer(new ExecutionConfig());
	}

	@Test
	public void testOneInput() throws Exception {
		TestingOneInputStreamOperator op = createOneInputStreamOperator();
		OneInputStreamOperatorOutput output = new OneInputStreamOperatorOutput(op);

		output.collect(element);
		assertEquals(element, op.getCurrentElement());

		output.emitWatermark(watermark);
		assertEquals(watermark, op.getCurrentWatermark());

		output.emitLatencyMarker(latencyMarker);
		assertEquals(latencyMarker, op.getCurrentLatencyMarker());
	}

	@Test
	public void testCopyingOneInput() throws Exception {
		TestingOneInputStreamOperator op = createOneInputStreamOperator();
		CopyingOneInputStreamOperatorOutput output = new CopyingOneInputStreamOperatorOutput(op, serializer);

		output.collect(element);
		assertNotSame(element, op.getCurrentElement());
		assertEquals(element, op.getCurrentElement());

		output.emitWatermark(watermark);
		assertSame(watermark, op.getCurrentWatermark());

		output.emitLatencyMarker(latencyMarker);
		assertSame(latencyMarker, op.getCurrentLatencyMarker());
	}

	@Test
	public void testFirstInputOfTwoInput() throws Exception {
		TestingTwoInputStreamOperator op = createTwoInputStreamOperator();
		FirstInputOfTwoInputStreamOperatorOutput output = new FirstInputOfTwoInputStreamOperatorOutput(op);

		output.collect(element);
		assertEquals(element, op.getCurrentElement1());
		assertNull(op.getCurrentElement2());

		output.emitWatermark(watermark);
		assertEquals(watermark, op.getCurrentWatermark1());
		assertNull(op.getCurrentWatermark2());

		output.emitLatencyMarker(latencyMarker);
		assertEquals(latencyMarker, op.getCurrentLatencyMarker1());
		assertNull(op.getCurrentLatencyMarker2());
	}

	@Test
	public void testCopyingFirstInputOfTwoInput() throws Exception {
		TestingTwoInputStreamOperator op = createTwoInputStreamOperator();
		CopyingFirstInputOfTwoInputStreamOperatorOutput output = new CopyingFirstInputOfTwoInputStreamOperatorOutput(op,
				serializer);

		output.collect(element);
		assertNotSame(element, op.getCurrentElement1());
		assertEquals(element, op.getCurrentElement1());
		assertNull(op.getCurrentElement2());

		output.emitWatermark(watermark);
		assertSame(watermark, op.getCurrentWatermark1());
		assertNull(op.getCurrentWatermark2());

		output.emitLatencyMarker(latencyMarker);
		assertSame(latencyMarker, op.getCurrentLatencyMarker1());
		assertNull(op.getCurrentLatencyMarker2());
	}

	@Test
	public void testSecondInputOfTwoInput() throws Exception {
		TestingTwoInputStreamOperator op = createTwoInputStreamOperator();
		SecondInputOfTwoInputStreamOperatorOutput output = new SecondInputOfTwoInputStreamOperatorOutput(op);

		output.collect(element);
		assertEquals(element, op.getCurrentElement2());
		assertNull(op.getCurrentElement1());

		output.emitWatermark(watermark);
		assertEquals(watermark, op.getCurrentWatermark2());
		assertNull(op.getCurrentWatermark1());

		output.emitLatencyMarker(latencyMarker);
		assertEquals(latencyMarker, op.getCurrentLatencyMarker2());
		assertNull(op.getCurrentLatencyMarker1());
	}

	@Test
	public void testCopyingSecondInputOfTwoInput() throws Exception {
		TestingTwoInputStreamOperator op = createTwoInputStreamOperator();
		CopyingSecondInputOfTwoInputStreamOperatorOutput output =
				new CopyingSecondInputOfTwoInputStreamOperatorOutput(op, serializer);

		output.collect(element);
		assertNotSame(element, op.getCurrentElement2());
		assertEquals(element, op.getCurrentElement2());
		assertNull(op.getCurrentElement1());

		output.emitWatermark(watermark);
		assertSame(watermark, op.getCurrentWatermark2());
		assertNull(op.getCurrentWatermark1());

		output.emitLatencyMarker(latencyMarker);
		assertSame(latencyMarker, op.getCurrentLatencyMarker2());
		assertNull(op.getCurrentLatencyMarker1());
	}

	@Test
	public void testBroadcasting() throws Exception {
		TestingOneInputStreamOperator op1 = createOneInputStreamOperator();
		TestingOneInputStreamOperator op2 = createOneInputStreamOperator();
		BroadcastingOutput output = new BroadcastingOutput(new Output[] {
				new OneInputStreamOperatorOutput(op1),
				new OneInputStreamOperatorOutput(op2) });

		output.collect(element);
		assertEquals(element, op1.getCurrentElement());
		assertEquals(element, op2.getCurrentElement());

		output.emitWatermark(watermark);
		assertEquals(watermark, op1.getCurrentWatermark());
		assertEquals(watermark, op2.getCurrentWatermark());

		// random choose one output to emit LatencyMarker
		output.emitLatencyMarker(latencyMarker);
		if (op1.getCurrentLatencyMarker() != null) {
			assertEquals(latencyMarker, op1.getCurrentLatencyMarker());
			assertNull(op2.getCurrentLatencyMarker());
		} else {
			assertEquals(latencyMarker, op2.getCurrentLatencyMarker());
		}
	}

	@Test
	public void testCopyingBroadcasting() throws Exception {
		TestingOneInputStreamOperator op1 = createOneInputStreamOperator();
		TestingOneInputStreamOperator op2 = createOneInputStreamOperator();
		CopyingBroadcastingOutput output = new CopyingBroadcastingOutput(new Output[] {
				new OneInputStreamOperatorOutput(op1),
				new OneInputStreamOperatorOutput(op2) });

		output.collect(element);
		assertNotSame(element, op1.getCurrentElement());
		assertEquals(element, op1.getCurrentElement());
		// the last element does not need copy
		assertSame(element, op2.getCurrentElement());

		output.emitWatermark(watermark);
		assertSame(watermark, op1.getCurrentWatermark());
		assertSame(watermark, op2.getCurrentWatermark());

		// random choose one output to emit LatencyMarker
		output.emitLatencyMarker(latencyMarker);
		if (op1.getCurrentLatencyMarker() != null) {
			assertSame(latencyMarker, op1.getCurrentLatencyMarker());
			assertNull(op2.getCurrentLatencyMarker());
		} else {
			assertSame(latencyMarker, op2.getCurrentLatencyMarker());
		}
	}

}
