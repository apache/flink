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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.metrics.MinWatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusSubMaintainer;

import org.junit.Test;

import java.util.BitSet;

import static org.apache.flink.streaming.runtime.io.OneInputProcessorTest.FakeStreamStatusMaintainer;
import static org.apache.flink.streaming.runtime.io.OneInputProcessorTest.getOperatorMetricGroup;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test for {@link SecondOfTwoInputProcessor}.
 */
public class SecondOfTwoInputProcessorTest {

	@Test
	public void testHandleWatermark() throws Exception {
		final FakeStreamStatusMaintainer parentMaintainer = new FakeStreamStatusMaintainer();
		final BitSet bitSet = new BitSet();
		final StreamStatusSubMaintainer subMaintainer = new StreamStatusSubMaintainer(
			parentMaintainer,
			bitSet,
			0);
		final TwoInputStreamOperator operator = mock(TwoInputStreamOperator.class);
		final OperatorMetricGroup operatorMetricGroup = getOperatorMetricGroup();
		operatorMetricGroup.getIOMetricGroup().reuseInputMetricsForTask();
		when(operator.getMetricGroup()).thenReturn(operatorMetricGroup);

		final MinWatermarkGauge minWatermarkGauge = new MinWatermarkGauge();
		final SecondOfTwoInputProcessor processor = new SecondOfTwoInputProcessor(
			subMaintainer,
			operator,
			this,
			operatorMetricGroup.parent(),
			minWatermarkGauge,
			2);

		// There are 2 channels
		final Watermark streamElement1 = new Watermark(123L);
		processor.processWatermark(streamElement1, 1);
		assertEquals(Long.MIN_VALUE, processor.getWatermarkProcessor().getInput2WatermarkGauge().getValue().longValue());

		final Watermark streamElement2 = new Watermark(234L);
		processor.processWatermark(streamElement2, 0);
		assertEquals(123L, processor.getWatermarkProcessor().getInput2WatermarkGauge().getValue().longValue());

		verify(operator, times(1)).processWatermark2(streamElement1.asWatermark());

		assertEquals(0L, operatorMetricGroup.getIOMetricGroup().getNumRecordsInCounter().getCount());
		assertEquals(0L, operatorMetricGroup.parent().getIOMetricGroup().getNumRecordsInCounter().getCount());
	}

	@Test
	public void testHandleStreamStatus() throws Exception {
		final FakeStreamStatusMaintainer parentMaintainer = new FakeStreamStatusMaintainer();
		final BitSet bitSet = new BitSet();
		final StreamStatusSubMaintainer subMaintainer = new StreamStatusSubMaintainer(
			parentMaintainer,
			bitSet,
			0);
		final TwoInputStreamOperator operator = mock(TwoInputStreamOperator.class);
		final OperatorMetricGroup operatorMetricGroup = getOperatorMetricGroup();
		operatorMetricGroup.getIOMetricGroup().reuseInputMetricsForTask();
		when(operator.getMetricGroup()).thenReturn(operatorMetricGroup);

		final MinWatermarkGauge minWatermarkGauge = new MinWatermarkGauge();
		final SecondOfTwoInputProcessor processor = new SecondOfTwoInputProcessor(
			subMaintainer,
			operator,
			this,
			operatorMetricGroup.parent(),
			minWatermarkGauge,
			2);

		// There are 2 channels
		final StreamStatus streamElement1 = new StreamStatus(StreamStatus.IDLE_STATUS);
		processor.processStreamStatus(streamElement1, 1);
		assertEquals(StreamStatus.ACTIVE, subMaintainer.getStreamStatus());
		assertEquals(StreamStatus.ACTIVE, parentMaintainer.getStreamStatus());

		final StreamStatus streamElement2 = new StreamStatus(StreamStatus.IDLE_STATUS);
		processor.processStreamStatus(streamElement2, 0);
		assertEquals(StreamStatus.IDLE, subMaintainer.getStreamStatus());
		assertEquals(StreamStatus.IDLE, parentMaintainer.getStreamStatus());

		assertEquals(0L, operatorMetricGroup.getIOMetricGroup().getNumRecordsInCounter().getCount());
		assertEquals(0L, operatorMetricGroup.parent().getIOMetricGroup().getNumRecordsInCounter().getCount());
	}

	@Test
	public void testProcessRecord() throws Exception {
		final FakeStreamStatusMaintainer parentMaintainer = new FakeStreamStatusMaintainer();
		final BitSet bitSet = new BitSet();
		final StreamStatusSubMaintainer subMaintainer = new StreamStatusSubMaintainer(
			parentMaintainer,
			bitSet,
			0);
		final TwoInputStreamOperator operator = mock(TwoInputStreamOperator.class);
		final OperatorMetricGroup operatorMetricGroup = getOperatorMetricGroup();
		operatorMetricGroup.getIOMetricGroup().reuseInputMetricsForTask();
		when(operator.getMetricGroup()).thenReturn(operatorMetricGroup);

		final MinWatermarkGauge minWatermarkGauge = new MinWatermarkGauge();
		final SecondOfTwoInputProcessor processor = new SecondOfTwoInputProcessor(
			subMaintainer,
			operator,
			this,
			operatorMetricGroup.parent(),
			minWatermarkGauge,
			2);

		// There are 2 channels
		final StreamRecord streamElement1 = new StreamRecord<>(123L);
		processor.processRecord(streamElement1, 1);

		final StreamRecord streamElement2 = new StreamRecord<>(234L);
		processor.processRecord(streamElement2, 0);

		//noinspection unchecked
		verify(operator, times(2)).setKeyContextElement2(any(StreamRecord.class));
		//noinspection unchecked
		verify(operator, times(2)).processElement2(any(StreamRecord.class));

		assertEquals(2, operatorMetricGroup.getIOMetricGroup().getNumRecordsInCounter().getCount());
		assertEquals(2, operatorMetricGroup.parent().getIOMetricGroup().getNumRecordsInCounter().getCount());
	}

	@Test
	public void testProcessLatencyMarker() throws Exception {
		final FakeStreamStatusMaintainer parentMaintainer = new FakeStreamStatusMaintainer();
		final BitSet bitSet = new BitSet();
		final StreamStatusSubMaintainer subMaintainer = new StreamStatusSubMaintainer(
			parentMaintainer,
			bitSet,
			0);
		final TwoInputStreamOperator operator = mock(TwoInputStreamOperator.class);
		final OperatorMetricGroup operatorMetricGroup = getOperatorMetricGroup();
		operatorMetricGroup.getIOMetricGroup().reuseInputMetricsForTask();
		when(operator.getMetricGroup()).thenReturn(operatorMetricGroup);

		final MinWatermarkGauge minWatermarkGauge = new MinWatermarkGauge();
		final SecondOfTwoInputProcessor processor = new SecondOfTwoInputProcessor(
			subMaintainer,
			operator,
			this,
			operatorMetricGroup.parent(),
			minWatermarkGauge,
			2);

		// There are 2 channels
		final LatencyMarker streamElement1 = new LatencyMarker(123L, new OperatorID(), 1);
		processor.processLatencyMarker(streamElement1, 1);

		final LatencyMarker streamElement2 = new LatencyMarker(234L, new OperatorID(), 0);
		processor.processLatencyMarker(streamElement2, 0);

		verify(operator, times(2)).processLatencyMarker2(any(LatencyMarker.class));
		assertEquals(0, operatorMetricGroup.getIOMetricGroup().getNumRecordsInCounter().getCount());
		assertEquals(0, operatorMetricGroup.parent().getIOMetricGroup().getNumRecordsInCounter().getCount());
	}
}
