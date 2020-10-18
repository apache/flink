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
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.Writer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;

/**
 * Base class for Tests for subclasses of {@link AbstractWriterOperator}.
 */
public abstract class WriterOperatorTestBase extends TestLogger {

	protected abstract <InputT, CommT> AbstractWriterOperatorFactory<InputT, CommT> createWriterOperator(
			TestSink<InputT, CommT, ?, ?> sink);

	@Test
	public void nonBufferingWriterEmitsWithoutFlush() throws Exception {
		final long initialTime = 0;

		final OneInputStreamOperatorTestHarness<Integer, Tuple3<Integer, Long, Long>> testHarness =
				createTestHarness(TestSink.create(NonBufferingWriter::new, stateSerializer()));
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
						new StreamRecord<>(Tuple3.of(1, initialTime + 1, initialTime)),
						new StreamRecord<>(Tuple3.of(2, initialTime + 2, initialTime))));
	}

	@Test
	public void nonBufferingWriterEmitsOnFlush() throws Exception {
		final long initialTime = 0;

		final OneInputStreamOperatorTestHarness<Integer, Tuple3<Integer, Long, Long>> testHarness =
				createTestHarness(TestSink.create(NonBufferingWriter::new, stateSerializer()));
		testHarness.open();

		testHarness.processWatermark(initialTime);
		testHarness.processElement(1, initialTime + 1);
		testHarness.processElement(2, initialTime + 2);

		testHarness.endInput();

		assertThat(
				testHarness.getOutput(),
				contains(
						new Watermark(initialTime),
						new StreamRecord<>(Tuple3.of(1, initialTime + 1, initialTime)),
						new StreamRecord<>(Tuple3.of(2, initialTime + 2, initialTime))));
	}

	@Test
	public void bufferingWriterDoesNotEmitWithoutFlush() throws Exception {
		final long initialTime = 0;

		final OneInputStreamOperatorTestHarness<Integer, Tuple3<Integer, Long, Long>> testHarness =
				createTestHarness(TestSink.create(BufferingWriter::new, stateSerializer()));
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

		final OneInputStreamOperatorTestHarness<Integer, Tuple3<Integer, Long, Long>> testHarness =
				createTestHarness(TestSink.create(BufferingWriter::new, stateSerializer()));
		testHarness.open();

		testHarness.processWatermark(initialTime);
		testHarness.processElement(1, initialTime + 1);
		testHarness.processElement(2, initialTime + 2);

		testHarness.endInput();

		assertThat(
				testHarness.getOutput(),
				contains(
						new Watermark(initialTime),
						new StreamRecord<>(Tuple3.of(1, initialTime + 1, initialTime)),
						new StreamRecord<>(Tuple3.of(2, initialTime + 2, initialTime))));
	}

	/**
	 * A {@link Writer} that returns all committables from {@link #prepareCommit(boolean)} without
	 * waiting for {@code flush} to be {@code true}.
	 */
	static class NonBufferingWriter extends TestWriter {
		@Override
		public List<Tuple3<Integer, Long, Long>> prepareCommit(boolean flush) {
			List<Tuple3<Integer, Long, Long>> result = elements;
			elements = new ArrayList<>();
			return result;
		}
	}

	/**
	 * A {@link Writer} that only returns committables from {@link #prepareCommit(boolean)} when
	 * {@code flush} is {@code true}.
	 */
	static class BufferingWriter extends TestWriter {
		@Override
		public List<Tuple3<Integer, Long, Long>> prepareCommit(boolean flush) {
			if (flush) {
				List<Tuple3<Integer, Long, Long>> result = elements;
				elements = new ArrayList<>();
				return result;
			} else {
				return Collections.emptyList();
			}
		}
	}

	/**
	 * Base class for out testing {@link Writer Writers}.
	 */
	abstract static class TestWriter
			implements Writer<Integer, Tuple3<Integer, Long, Long>, Tuple3<Integer, Long, Long>> {

		// element, timestamp, watermark
		protected List<Tuple3<Integer, Long, Long>> elements;

		TestWriter() {
			this.elements = new ArrayList<>();
		}

		@Override
		public void write(Integer element, Context context) {
			elements.add(Tuple3.of(element, context.timestamp(), context.currentWatermark()));
		}

		@Override
		public List<Tuple3<Integer, Long, Long>> snapshotState() {
			return Collections.emptyList();
		}

		@Override
		public void close() throws Exception {
		}
	}

	/**
	 * A {@link Sink} for testing that uses {@link Supplier Suppliers} to create various components
	 * under test.
	 */
	protected static class TestSink<InputT, CommT, WriterStateT, GlobalCommT>
			implements Sink<InputT, CommT, WriterStateT, GlobalCommT> {

		private final Function<List<WriterStateT>, Writer<InputT, CommT, WriterStateT>> writerSupplier;
		private final Supplier<Optional<SimpleVersionedSerializer<WriterStateT>>> writerStateSerializerSupplier;

		public static <InputT, CommT, WriterStateT, GlobalCommT> TestSink<InputT, CommT, WriterStateT, GlobalCommT> create(
				Supplier<Writer<InputT, CommT, WriterStateT>> writer) {
			// We cannot replace this by a method reference because the Java compiler will not be
			// able to typecheck it.
			//noinspection Convert2MethodRef
			return new TestSink<>((state) -> writer.get(), () -> Optional.empty());
		}

		public static <InputT, CommT, WriterStateT, GlobalCommT> TestSink<InputT, CommT, WriterStateT, GlobalCommT> create(
				Supplier<Writer<InputT, CommT, WriterStateT>> writer,
				Supplier<Optional<SimpleVersionedSerializer<WriterStateT>>> writerStateSerializerSupplier) {
			return new TestSink<>((state) -> writer.get(), writerStateSerializerSupplier);
		}

		public static <InputT, CommT, WriterStateT, GlobalCommT> TestSink<InputT, CommT, WriterStateT, GlobalCommT> create(
				Function<List<WriterStateT>, Writer<InputT, CommT, WriterStateT>> writer,
				Supplier<Optional<SimpleVersionedSerializer<WriterStateT>>> writerStateSerializerSupplier) {
			return new TestSink<>(writer, writerStateSerializerSupplier);
		}

		private TestSink(
				Function<List<WriterStateT>, Writer<InputT, CommT, WriterStateT>> writer,
				Supplier<Optional<SimpleVersionedSerializer<WriterStateT>>> writerStateSerializerSupplier) {
			this.writerSupplier = writer;
			this.writerStateSerializerSupplier = writerStateSerializerSupplier;
		}

		public Function<List<WriterStateT>, Writer<InputT, CommT, WriterStateT>> getWriterSupplier() {
			return writerSupplier;
		}

		public Supplier<Optional<SimpleVersionedSerializer<WriterStateT>>> getWriterStateSerializerSupplier() {
			return writerStateSerializerSupplier;
		}

		@Override
		public Writer<InputT, CommT, WriterStateT> createWriter(
				InitContext context,
				List<WriterStateT> states) {
			return writerSupplier.apply(states);
		}

		@Override
		public Optional<Committer<CommT>> createCommitter() {
			return Optional.empty();
		}

		@Override
		public Optional<GlobalCommitter<CommT, GlobalCommT>> createGlobalCommitter() {
			return Optional.empty();
		}

		@Override
		public Optional<SimpleVersionedSerializer<CommT>> getCommittableSerializer() {
			return Optional.empty();
		}

		@Override
		public Optional<SimpleVersionedSerializer<GlobalCommT>> getGlobalCommittableSerializer() {
			return Optional.empty();
		}

		@Override
		public Optional<SimpleVersionedSerializer<WriterStateT>> getWriterStateSerializer() {
			return writerStateSerializerSupplier.get();
		}
	}

	public static Supplier<Optional<SimpleVersionedSerializer<Tuple3<Integer, Long, Long>>>> stateSerializer() {
		return () -> Optional.of(new WriterStateSerializer());
	}

	static final class WriterStateSerializer implements SimpleVersionedSerializer<Tuple3<Integer, Long, Long>> {

		@Override
		public int getVersion() {
			return 0;
		}

		@Override
		public byte[] serialize(Tuple3<Integer, Long, Long> tuple3) throws IOException {
			return InstantiationUtil.serializeObject(tuple3);

		}

		@Override
		public Tuple3<Integer, Long, Long> deserialize(
				int version,
				byte[] serialized) throws IOException {
			try {
				return InstantiationUtil.deserializeObject(serialized, getClass().getClassLoader());
			} catch (ClassNotFoundException e) {
				throw new RuntimeException("Failed to deserialize the writer's state.", e);
			}
		}
	}

	protected <CommT> OneInputStreamOperatorTestHarness<Integer, CommT> createTestHarness(
			TestSink<Integer, CommT, ?, ?> sink) throws Exception {

		return new OneInputStreamOperatorTestHarness<>(
				createWriterOperator(sink),
				IntSerializer.INSTANCE);
	}
}
