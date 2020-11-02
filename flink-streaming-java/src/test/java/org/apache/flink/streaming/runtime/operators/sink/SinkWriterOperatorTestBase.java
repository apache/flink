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

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;

/**
 * Base class for Tests for subclasses of {@link AbstractSinkWriterOperator}.
 */
public abstract class SinkWriterOperatorTestBase extends TestLogger {

	protected abstract AbstractSinkWriterOperatorFactory<Integer, String> createWriterOperator(TestSink sink);

	@Test
	public void nonBufferingWriterEmitsWithoutFlush() throws Exception {
		final long initialTime = 0;

		final OneInputStreamOperatorTestHarness<Integer, String> testHarness =
				createTestHarness(TestSink
						.newBuilder()
						.setWriter(new NonBufferingSinkWriter())
						.withWriterState()
						.build());
		testHarness.open();

		testHarness.processWatermark(initialTime);
		testHarness.processElement(1, initialTime + 1);
		testHarness.processElement(2, initialTime + 2);

		testHarness.prepareSnapshotPreBarrier(1L);
		testHarness.snapshot(1L, 1L);

		assertThat(
				testHarness.getOutput(),
				contains(
						new Watermark(initialTime),
						new StreamRecord<>(Tuple3.of(1, initialTime + 1, initialTime).toString()),
						new StreamRecord<>(Tuple3.of(2, initialTime + 2, initialTime).toString())));
	}

	@Test
	public void nonBufferingWriterEmitsOnFlush() throws Exception {
		final long initialTime = 0;

		final OneInputStreamOperatorTestHarness<Integer, String> testHarness =
				createTestHarness(TestSink
						.newBuilder()
						.setWriter(new NonBufferingSinkWriter())
						.withWriterState()
						.build());
		testHarness.open();

		testHarness.processWatermark(initialTime);
		testHarness.processElement(1, initialTime + 1);
		testHarness.processElement(2, initialTime + 2);

		testHarness.endInput();

		assertThat(
				testHarness.getOutput(),
				contains(
						new Watermark(initialTime),
						new StreamRecord<>(Tuple3.of(1, initialTime + 1, initialTime).toString()),
						new StreamRecord<>(Tuple3.of(2, initialTime + 2, initialTime).toString())));
	}

	@Test
	public void bufferingWriterDoesNotEmitWithoutFlush() throws Exception {
		final long initialTime = 0;

		final OneInputStreamOperatorTestHarness<Integer, String> testHarness =
				createTestHarness(TestSink
						.newBuilder()
						.setWriter(new BufferingSinkWriter())
						.withWriterState()
						.build());
		testHarness.open();

		testHarness.processWatermark(initialTime);
		testHarness.processElement(1, initialTime + 1);
		testHarness.processElement(2, initialTime + 2);

		testHarness.prepareSnapshotPreBarrier(1L);
		testHarness.snapshot(1L, 1L);

		assertThat(
				testHarness.getOutput(),
				contains(
						new Watermark(initialTime)));
	}

	@Test
	public void bufferingWriterEmitsOnFlush() throws Exception {
		final long initialTime = 0;

		final OneInputStreamOperatorTestHarness<Integer, String> testHarness =
				createTestHarness(TestSink
						.newBuilder()
						.setWriter(new BufferingSinkWriter())
						.withWriterState()
						.build());
		testHarness.open();

		testHarness.processWatermark(initialTime);
		testHarness.processElement(1, initialTime + 1);
		testHarness.processElement(2, initialTime + 2);

		testHarness.endInput();

		assertThat(
				testHarness.getOutput(),
				contains(
						new Watermark(initialTime),
						new StreamRecord<>(Tuple3.of(1, initialTime + 1, initialTime).toString()),
						new StreamRecord<>(Tuple3.of(2, initialTime + 2, initialTime).toString())));
	}

	/**
	 * A {@link SinkWriter} that returns all committables from {@link #prepareCommit(boolean)} without
	 * waiting for {@code flush} to be {@code true}.
	 */
	static class NonBufferingSinkWriter extends TestSink.DefaultSinkWriter {
		@Override
		public List<String> prepareCommit(boolean flush) {
			List<String> result = elements;
			elements = new ArrayList<>();
			return result;
		}
	}

	/**
	 * A {@link SinkWriter} that only returns committables from {@link #prepareCommit(boolean)} when
	 * {@code flush} is {@code true}.
	 */
	static class BufferingSinkWriter extends TestSink.DefaultSinkWriter {
		@Override
		public List<String> prepareCommit(boolean flush) {
			if (!flush) {
				return Collections.emptyList();
			}
			List<String> result = elements;
			elements = new ArrayList<>();
			return result;
		}
	}

	protected OneInputStreamOperatorTestHarness<Integer, String> createTestHarness(TestSink sink) throws Exception {
		return new OneInputStreamOperatorTestHarness<>(
				createWriterOperator(sink),
				IntSerializer.INSTANCE);
	}
}
