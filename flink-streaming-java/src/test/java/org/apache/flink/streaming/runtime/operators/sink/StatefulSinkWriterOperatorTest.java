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

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link StatefulSinkWriterOperator}.
 */
public class StatefulSinkWriterOperatorTest extends SinkWriterOperatorTestBase {

	@Override
	protected AbstractSinkWriterOperatorFactory createWriterOperator(TestSink sink) {
		return new StatefulSinkWriterOperatorFactory<>(sink);
	}

	@Test
	public void stateIsRestored() throws Exception {
		final long initialTime = 0;

		final OneInputStreamOperatorTestHarness<Integer, String> testHarness =
				createTestHarness(TestSink
						.newBuilder()
						.setWriter(new SnapshottingBufferingSinkWriter())
						.withWriterState()
						.build());

		testHarness.open();

		testHarness.processWatermark(initialTime);
		testHarness.processElement(1, initialTime + 1);
		testHarness.processElement(2, initialTime + 2);

		testHarness.prepareSnapshotPreBarrier(1L);
		OperatorSubtaskState snapshot = testHarness.snapshot(1L, 1L);

		// we only see the watermark, so the committables must be stored in state
		assertThat(
				testHarness.getOutput(),
				contains(
						new Watermark(initialTime)));

		testHarness.close();

		final OneInputStreamOperatorTestHarness<Integer, String> restoredTestHarness =
				createTestHarness(TestSink
						.newBuilder()
						.setWriter(new SnapshottingBufferingSinkWriter())
						.withWriterState()
						.build());

		restoredTestHarness.initializeState(snapshot);
		restoredTestHarness.open();

		// this will flush out the committables that were restored
		restoredTestHarness.endInput();

		assertThat(
				restoredTestHarness.getOutput(),
				contains(
						new StreamRecord<>(Tuple3.of(1, initialTime + 1, initialTime).toString()),
						new StreamRecord<>(Tuple3.of(2, initialTime + 2, initialTime).toString())));
	}

	/**
	 * A {@link SinkWriter} buffers elements and snapshots them when asked.
	 */
	static class SnapshottingBufferingSinkWriter extends BufferingSinkWriter {

		@Override
		public List<String> snapshotState() {
			return elements;
		}

		@Override
		void restoredFrom(List<String> states) {
			this.elements = states;
		}
	}
}
