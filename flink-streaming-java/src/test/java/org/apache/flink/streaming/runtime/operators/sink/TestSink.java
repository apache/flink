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

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.Writer;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A {@link Sink} for testing that uses {@link Supplier Suppliers} to create various components
 * under test.
 */
class TestSink<InputT, CommT, WriterStateT, GlobalCommT>
		implements Sink<InputT, CommT, WriterStateT, GlobalCommT> {

	static final DefaultWriter<String> DEFAULT_WRITER = new DefaultWriter<>();

	private final Function<List<WriterStateT>, Writer<InputT, CommT, WriterStateT>> writerSupplier;
	private final Supplier<Optional<SimpleVersionedSerializer<WriterStateT>>> writerStateSerializerSupplier;

	private final Supplier<Optional<Committer<CommT>>> committerSupplier;
	private final Supplier<Optional<SimpleVersionedSerializer<CommT>>> committableSerializerSupplier;

	private final Supplier<Optional<GlobalCommitter<CommT, GlobalCommT>>> globalCommitterSupplier;
	private final Supplier<Optional<SimpleVersionedSerializer<GlobalCommT>>> globalCommitterSerializerSupplier;

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

	public static <InputT, CommT, WriterStateT, GlobalCommT> TestSink<InputT, CommT, WriterStateT, GlobalCommT> create(
			Supplier<Writer<InputT, CommT, WriterStateT>> writer,
			Supplier<Optional<Committer<CommT>>> committerSupplier,
			Supplier<Optional<SimpleVersionedSerializer<CommT>>> committableSerializerSupplier,
			Supplier<Optional<GlobalCommitter<CommT, GlobalCommT>>> globalCommitterSupplier,
			Supplier<Optional<SimpleVersionedSerializer<GlobalCommT>>> globalCommittableSerializer) {
		return new TestSink<>(
				(s) -> writer.get(),
				() -> Optional.empty(),
				committerSupplier,
				committableSerializerSupplier,
				globalCommitterSupplier,
				globalCommittableSerializer);
	}

	private TestSink(
			Function<List<WriterStateT>, Writer<InputT, CommT, WriterStateT>> writerSupplier,
			Supplier<Optional<SimpleVersionedSerializer<WriterStateT>>> writerStateSerializerSupplier,
			Supplier<Optional<Committer<CommT>>> committerSupplier,
			Supplier<Optional<SimpleVersionedSerializer<CommT>>> committableSerializerSupplier,
			Supplier<Optional<GlobalCommitter<CommT, GlobalCommT>>> globalCommitterSupplier,
			Supplier<Optional<SimpleVersionedSerializer<GlobalCommT>>> globalCommitterSerializerSupplier) {
		this.writerSupplier = writerSupplier;
		this.writerStateSerializerSupplier = writerStateSerializerSupplier;
		this.committerSupplier = committerSupplier;
		this.committableSerializerSupplier = committableSerializerSupplier;
		this.globalCommitterSupplier = globalCommitterSupplier;
		this.globalCommitterSerializerSupplier = globalCommitterSerializerSupplier;
	}

	private TestSink(
			Function<List<WriterStateT>, Writer<InputT, CommT, WriterStateT>> writer,
			Supplier<Optional<SimpleVersionedSerializer<WriterStateT>>> writerStateSerializerSupplier) {
		this(
				writer,
				writerStateSerializerSupplier,
				Optional::empty,
				Optional::empty,
				Optional::empty,
				Optional::empty);
	}

	@Override
	public Writer<InputT, CommT, WriterStateT> createWriter(
			InitContext context,
			List<WriterStateT> states) {
		return writerSupplier.apply(states);
	}

	@Override
	public Optional<Committer<CommT>> createCommitter() {
		return committerSupplier.get();
	}

	@Override
	public Optional<GlobalCommitter<CommT, GlobalCommT>> createGlobalCommitter() {
		return globalCommitterSupplier.get();
	}

	@Override
	public Optional<SimpleVersionedSerializer<CommT>> getCommittableSerializer() {
		return committableSerializerSupplier.get();
	}

	@Override
	public Optional<SimpleVersionedSerializer<GlobalCommT>> getGlobalCommittableSerializer() {
		return globalCommitterSerializerSupplier.get();
	}

	@Override
	public Optional<SimpleVersionedSerializer<WriterStateT>> getWriterStateSerializer() {
		return writerStateSerializerSupplier.get();
	}

	/**
	 * This is default writer used for testing {@link Committer} and {@link GlobalCommitter}'s operator.
	 */
	static class DefaultWriter<CommT> implements Writer<CommT, CommT, CommT> {

		@Override
		public void write(CommT element, Context context) {
			//do nothing
		}

		@Override
		public List<CommT> prepareCommit(boolean flush) {
			return Collections.emptyList();
		}

		@Override
		public List<CommT> snapshotState() {
			return Collections.emptyList();
		}

		@Override
		public void close() throws Exception {

		}
	}

	/**
	 * Base class for testing {@link Committer} and {@link GlobalCommitter}.
	 */
	abstract static class AbstractTestCommitter<CommT> implements Committer<CommT> {

		protected List<CommT> committedData;

		private boolean isClosed;

		public AbstractTestCommitter() {
			this.committedData = new ArrayList<>();
			this.isClosed = false;
		}

		public List<CommT> getCommittedData() {
			return committedData;
		}

		@Override
		public void close() throws Exception {
			isClosed = true;
		}

		public boolean isClosed() {
			return isClosed;
		}
	}

	/**
	 * The class used for normal committer test.
	 */
	static class TestCommitter extends AbstractTestCommitter<String> {

		@Override
		public List<String> commit(List<String> committables) {
			if (committedData != null) {
				committedData.addAll(committables);
			}
			return Collections.emptyList();
		}
	}

	/**
	 * This committer always re-commits the committables data it received.
	 */
	static class AlwaysRetryCommitter extends AbstractTestCommitter<String> {

		@Override
		public List<String> commit(List<String> committables) {
			return committables;
		}
	}

	/**
	 * The class used for normal global committer test.
	 */
	static class TestGlobalCommitter extends TestCommitter implements GlobalCommitter<String, String> {

		static final Function<List<String>, String> COMBINER = (x) -> String.join("+", x);

		private final String committedSuccessData;

		TestGlobalCommitter(String committedSuccessData) {
			this.committedSuccessData = committedSuccessData;
		}

		@Override
		public List<String> filterRecoveredCommittables(List<String> globalCommittables) {
			if (committedSuccessData == null) {
				return globalCommittables;
			}
			return globalCommittables
					.stream()
					.filter(s -> !s.equals(committedSuccessData))
					.collect(Collectors.toList());
		}

		@Override
		public String combine(List<String> committables) {
			return COMBINER.apply(committables);
		}

		@Override
		public void endOfInput() {
			this.committedData.add("end of input");
		}
	}

	/**
	 * This global committer always re-commits the committables data it received.
	 */
	static class AlwaysRetryGlobalCommitter extends AbstractTestCommitter<String> implements GlobalCommitter<String, String> {

		@Override
		public List<String> filterRecoveredCommittables(List<String> globalCommittables) {
			return Collections.emptyList();
		}

		@Override
		public String combine(List<String> committables) {
			return String.join("|", committables);
		}

		@Override
		public void endOfInput() {

		}

		@Override
		public List<String> commit(List<String> committables) {
			return committables;
		}
	}
}
