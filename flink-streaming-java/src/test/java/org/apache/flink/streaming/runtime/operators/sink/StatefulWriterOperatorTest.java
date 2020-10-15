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

import org.apache.flink.api.connector.sink.Writer;
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
 * Tests for {@link StatefulWriterOperator}.
 */
public class StatefulWriterOperatorTest extends WriterOperatorTestBase {

	@Override
	protected <InputT, CommT> AbstractWriterOperatorFactory<InputT, CommT> createWriterOperator(
			TestSink<InputT, CommT, ?, ?> sink) {
		return new StatefulWriterOperatorFactory<>(sink);
	}

	@Test
	public void stateIsRestored() throws Exception {
		final long initialTime = 0;

		final OneInputStreamOperatorTestHarness<Integer, Tuple3<Integer, Long, Long>> testHarness =
				createTestHarness(TestSink.create(
						SnapshottingBufferingWriter::new,
						stateSerializer()));
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

		final OneInputStreamOperatorTestHarness<Integer, Tuple3<Integer, Long, Long>> restoredTestHarness =
				createTestHarness(TestSink.create(
						SnapshottingBufferingWriter::new,
						stateSerializer()));

		restoredTestHarness.initializeState(snapshot);
		restoredTestHarness.open();

		// this will flush out the committables that were restored
		restoredTestHarness.endInput();

		assertThat(
				restoredTestHarness.getOutput(),
				contains(
						new StreamRecord<>(Tuple3.of(1, initialTime + 1, initialTime)),
						new StreamRecord<>(Tuple3.of(2, initialTime + 2, initialTime))));
	}

	/**
	 * A {@link Writer} buffers elements and snapshots them when asked.
	 */
	static class SnapshottingBufferingWriter extends BufferingWriter {
		public SnapshottingBufferingWriter(List<Tuple3<Integer, Long, Long>> state) {
			this.elements = state;
		}

		@Override
		public List<Tuple3<Integer, Long, Long>> snapshotState() {
			return elements;
		}
	}
}
