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
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.junit.Assert.assertNotNull;

/**
 * A {@link Sink TestSink} for all the sink related tests.
 */
public class TestSink implements Sink<Integer, String, String, String> {

	private final DefaultSinkWriter writer;

	@Nullable
	private final SimpleVersionedSerializer<String> writerStateSerializer;

	@Nullable
	private final Committer<String> committer;

	@Nullable
	private final SimpleVersionedSerializer<String> committableSerializer;

	@Nullable
	private final GlobalCommitter<String, String> globalCommitter;

	@Nullable
	private final SimpleVersionedSerializer<String> globalCommittableSerializer;

	private TestSink(
			DefaultSinkWriter writer,
			@Nullable SimpleVersionedSerializer<String> writerStateSerializer,
			@Nullable Committer<String> committer,
			@Nullable SimpleVersionedSerializer<String> committableSerializer,
			@Nullable GlobalCommitter<String, String> globalCommitter,
			@Nullable SimpleVersionedSerializer<String> globalCommittableSerializer) {
		this.writer = writer;
		this.writerStateSerializer = writerStateSerializer;
		this.committer = committer;
		this.committableSerializer = committableSerializer;
		this.globalCommitter = globalCommitter;
		this.globalCommittableSerializer = globalCommittableSerializer;
	}

	@Override
	public SinkWriter<Integer, String, String> createWriter(
			InitContext context,
			List<String> states) {
		writer.restoredFrom(states);
		writer.setProcessingTimerService(context.getProcessingTimeService());
		return writer;
	}

	@Override
	public Optional<Committer<String>> createCommitter() {
		return Optional.ofNullable(committer);
	}

	@Override
	public Optional<GlobalCommitter<String, String>> createGlobalCommitter() {
		return Optional.ofNullable(globalCommitter);
	}

	@Override
	public Optional<SimpleVersionedSerializer<String>> getCommittableSerializer() {
		return Optional.ofNullable(committableSerializer);
	}

	@Override
	public Optional<SimpleVersionedSerializer<String>> getGlobalCommittableSerializer() {
		return Optional.ofNullable(globalCommittableSerializer);
	}

	@Override
	public Optional<SimpleVersionedSerializer<String>> getWriterStateSerializer() {
		return Optional.ofNullable(writerStateSerializer);
	}

	public static Builder newBuilder() {
		return new Builder();
	}

	/**
	 * A builder class for {@link TestSink}.
	 */
	public static class Builder {

		private DefaultSinkWriter writer = new DefaultSinkWriter();

		private SimpleVersionedSerializer<String> writerStateSerializer;

		private Committer<String> committer;

		private SimpleVersionedSerializer<String> committableSerializer;

		private GlobalCommitter<String, String> globalCommitter;

		private SimpleVersionedSerializer<String> globalCommittableSerializer;

		public Builder setWriter(DefaultSinkWriter writer) {
			this.writer = checkNotNull(writer);
			return this;
		}

		public Builder withWriterState() {
			this.writerStateSerializer = StringCommittableSerializer.INSTANCE;
			return this;
		}

		public Builder setCommitter(Committer<String> committer) {
			this.committer = committer;
			return this;
		}

		public Builder setCommittableSerializer(SimpleVersionedSerializer<String> committableSerializer) {
			this.committableSerializer = committableSerializer;
			return this;
		}

		public Builder setDefaultCommitter() {
			this.committer = new DefaultCommitter();
			this.committableSerializer = StringCommittableSerializer.INSTANCE;
			return this;
		}

		public Builder setDefaultCommitter(Supplier<Queue<String>> queueSupplier) {
			this.committer = new DefaultCommitter(queueSupplier);
			this.committableSerializer = StringCommittableSerializer.INSTANCE;
			return this;
		}

		public Builder setGlobalCommitter(GlobalCommitter<String, String> globalCommitter) {
			this.globalCommitter = globalCommitter;
			return this;
		}

		public Builder setGlobalCommittableSerializer(SimpleVersionedSerializer<String> globalCommittableSerializer) {
			this.globalCommittableSerializer = globalCommittableSerializer;
			return this;
		}

		public Builder setDefaultGlobalCommitter() {
			this.globalCommitter = new DefaultGlobalCommitter("");
			this.globalCommittableSerializer = StringCommittableSerializer.INSTANCE;
			return this;
		}

		public Builder setGlobalCommitter(Supplier<Queue<String>> queueSupplier) {
			this.globalCommitter = new DefaultGlobalCommitter(queueSupplier);
			this.globalCommittableSerializer = StringCommittableSerializer.INSTANCE;
			return this;
		}

		public TestSink build() {
			return new TestSink(
					writer,
					writerStateSerializer,
					committer,
					committableSerializer,
					globalCommitter,
					globalCommittableSerializer);
		}
	}

	// -------------------------------------- Sink Writer ------------------------------------------

	/**
	 * Base class for out testing {@link SinkWriter Writers}.
	 */
	static class DefaultSinkWriter implements SinkWriter<Integer, String, String>, Serializable {

		protected List<String> elements;

		protected ProcessingTimeService processingTimerService;

		DefaultSinkWriter() {
			this.elements = new ArrayList<>();
		}

		@Override
		public void write(Integer element, Context context) {
			elements.add(Tuple3
					.of(element, context.timestamp(), context.currentWatermark())
					.toString());
		}

		@Override
		public List<String> prepareCommit(boolean flush) {
			List<String> result = elements;
			elements = new ArrayList<>();
			return result;
		}

		@Override
		public List<String> snapshotState() {
			return Collections.emptyList();
		}

		@Override
		public void close() throws Exception {
		}

		void restoredFrom(List<String> states) {

		}

		void setProcessingTimerService(ProcessingTimeService processingTimerService) {
			this.processingTimerService = processingTimerService;
		}
	}

	// -------------------------------------- Sink Committer ---------------------------------------

	/**
	 * Base class for testing {@link Committer} and {@link GlobalCommitter}.
	 */
	static class DefaultCommitter implements Committer<String>, Serializable {

		@Nullable
		private Queue<String> committedData;

		private boolean isClosed;

		@Nullable
		private final Supplier<Queue<String>> queueSupplier;

		public DefaultCommitter() {
			this.committedData = new ConcurrentLinkedQueue<>();
			this.isClosed = false;
			this.queueSupplier = null;
		}

		public DefaultCommitter(@Nullable Supplier<Queue<String>> queueSupplier) {
			this.queueSupplier = queueSupplier;
			this.isClosed = false;
			this.committedData = null;
		}

		public List<String> getCommittedData() {
			if (committedData != null) {
				return new ArrayList<>(committedData);
			} else {
				return Collections.emptyList();
			}
		}

		@Override
		public List<String> commit(List<String> committables) {
			if (committedData == null) {
				assertNotNull(queueSupplier);
				committedData = queueSupplier.get();
			}
			committedData.addAll(committables);
			return Collections.emptyList();
		}

		public void close() throws Exception {
			isClosed = true;
		}

		public boolean isClosed() {
			return isClosed;
		}
	}

	/**
	 * A {@link Committer} that always re-commits the committables data it received.
	 */
	static class AlwaysRetryCommitter extends DefaultCommitter implements Committer<String> {

		@Override
		public List<String> commit(List<String> committables) {
			return committables;
		}
	}

	// ------------------------------------- Sink Global Committer ---------------------------------

	/**
	 * A {@link GlobalCommitter} that always commits global committables successfully.
	 */
	static class DefaultGlobalCommitter extends DefaultCommitter implements GlobalCommitter<String, String> {

		static final Function<List<String>, String> COMBINER = strings -> {
			//we sort here because we want to have a deterministic result during the unit test
			Collections.sort(strings);
			return String.join("+", strings);
		};

		private final String committedSuccessData;

		DefaultGlobalCommitter() {
			this("");
		}

		DefaultGlobalCommitter(String committedSuccessData) {
			this.committedSuccessData = committedSuccessData;
		}

		DefaultGlobalCommitter(Supplier<Queue<String>> queueSupplier) {
			super(queueSupplier);
			committedSuccessData = "";
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
			commit(Collections.singletonList("end of input"));
		}
	}

	/**
	 * A {@link GlobalCommitter} that always re-commits global committables it received.
	 */
	static class AlwaysRetryGlobalCommitter extends DefaultGlobalCommitter {

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

	/**
	 * We introduce this {@link StringCommittableSerializer} is because that all the fields of {@link TestSink} should be
	 * serializable.
	 */
	public static class StringCommittableSerializer implements SimpleVersionedSerializer<String>, Serializable {

		public static final StringCommittableSerializer INSTANCE = new StringCommittableSerializer();

		@Override
		public int getVersion() {
			return SimpleVersionedStringSerializer.INSTANCE.getVersion();
		}

		@Override
		public byte[] serialize(String obj) throws IOException {
			return SimpleVersionedStringSerializer.INSTANCE.serialize(obj);
		}

		@Override
		public String deserialize(int version, byte[] serialized) throws IOException {
			return SimpleVersionedStringSerializer.INSTANCE.deserialize(version, serialized);
		}
	}
}
