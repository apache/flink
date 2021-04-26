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

package org.apache.flink.test.streaming.runtime;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.IntegerTypeInfo;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.runtime.operators.sink.TestSink;
import org.apache.flink.streaming.util.FiniteTestSource;
import org.apache.flink.test.util.AbstractTestBase;

import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.joining;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

/**
 * Integration test for {@link org.apache.flink.api.connector.sink.Sink} run time implementation.
 */
public class SinkITCase extends AbstractTestBase {

	static final List<Integer> SOURCE_DATA = Arrays.asList(
			895, 127, 148, 161, 148, 662, 822, 491, 275, 122,
			850, 630, 682, 765, 434, 970, 714, 795, 288, 422);

	static final List<String> EXPECTED_COMMITTED_DATA_IN_STREAMING_MODE = SOURCE_DATA
			.stream()
			// source send data two times
			.flatMap(x -> Collections
					.nCopies(2, Tuple3.of(x, null, Long.MIN_VALUE).toString())
					.stream())
			.collect(
					Collectors.toList());

	static final List<String> EXPECTED_COMMITTED_DATA_IN_BATCH_MODE = SOURCE_DATA
			.stream()
			.map(x -> Tuple3.of(x, null, Long.MIN_VALUE).toString())
			.collect(
					Collectors.toList());

	static final List<String> EXPECTED_GLOBAL_COMMITTED_DATA_IN_STREAMING_MODE = Collections.nCopies(
			2,
			SOURCE_DATA
					.stream()
					.map(x -> Tuple3.of(x, null, Long.MIN_VALUE).toString())
					.sorted()
					.collect(joining("+")));

	static final List<String> EXPECTED_GLOBAL_COMMITTED_DATA_IN_BATCH_MODE = Arrays.asList(
			SOURCE_DATA
					.stream()
					.map(x -> Tuple3.of(x, null, Long.MIN_VALUE).toString())
					.sorted()
					.collect(joining("+")),
			"end of input");

	static final Queue<String> COMMIT_QUEUE = new ConcurrentLinkedQueue<>();

	static final Queue<String> GLOBAL_COMMIT_QUEUE = new ConcurrentLinkedQueue<>();

	@Before
	public void init() {
		COMMIT_QUEUE.clear();
		GLOBAL_COMMIT_QUEUE.clear();
	}

	@Test
	public void writerAndCommitterAndGlobalCommitterExecuteInStreamingMode() throws Exception {
		final StreamExecutionEnvironment env = buildStreamEnv();
		final FiniteTestSource<Integer> source = new FiniteTestSource<>(SOURCE_DATA);

		env.addSource(source, IntegerTypeInfo.INT_TYPE_INFO)
				.sinkTo(TestSink
						.newBuilder()
						.setDefaultCommitter((Supplier<Queue<String>> & Serializable) () -> COMMIT_QUEUE)
						.setGlobalCommitter((Supplier<Queue<String>> & Serializable) () -> GLOBAL_COMMIT_QUEUE)
						.build());

		env.execute();

		assertThat(
				COMMIT_QUEUE,
				containsInAnyOrder(EXPECTED_COMMITTED_DATA_IN_STREAMING_MODE.toArray()));

		assertThat(
				GLOBAL_COMMIT_QUEUE,
				containsInAnyOrder(EXPECTED_GLOBAL_COMMITTED_DATA_IN_STREAMING_MODE.toArray()));
	}

	@Test
	public void writerAndCommitterAndGlobalCommitterExecuteInBatchMode() throws Exception {
		final StreamExecutionEnvironment env = buildBatchEnv();

		env.fromCollection(SOURCE_DATA)
				.sinkTo(TestSink
						.newBuilder()
						.setDefaultCommitter((Supplier<Queue<String>> & Serializable) () -> COMMIT_QUEUE)
						.setGlobalCommitter((Supplier<Queue<String>> & Serializable) () -> GLOBAL_COMMIT_QUEUE)
						.build());

		env.execute();

		assertThat(
				COMMIT_QUEUE,
				containsInAnyOrder(EXPECTED_COMMITTED_DATA_IN_BATCH_MODE.toArray()));

		assertThat(
				GLOBAL_COMMIT_QUEUE,
				containsInAnyOrder(EXPECTED_GLOBAL_COMMITTED_DATA_IN_BATCH_MODE.toArray()));
	}

	@Test
	public void writerAndCommitterExecuteInStreamingMode() throws Exception {
		final StreamExecutionEnvironment env = buildStreamEnv();
		final FiniteTestSource<Integer> source = new FiniteTestSource<>(SOURCE_DATA);

		env.addSource(source, IntegerTypeInfo.INT_TYPE_INFO)
				.sinkTo(TestSink
						.newBuilder()
						.setDefaultCommitter((Supplier<Queue<String>> & Serializable) () -> COMMIT_QUEUE)
						.build());
		env.execute();
		assertThat(
				COMMIT_QUEUE,
				containsInAnyOrder(EXPECTED_COMMITTED_DATA_IN_STREAMING_MODE.toArray()));
	}

	@Test
	public void writerAndCommitterExecuteInBatchMode() throws Exception {
		final StreamExecutionEnvironment env = buildBatchEnv();

		env.fromCollection(SOURCE_DATA)
				.sinkTo(TestSink
						.newBuilder()
						.setDefaultCommitter((Supplier<Queue<String>> & Serializable) () -> COMMIT_QUEUE)
						.build());
		env.execute();
		assertThat(
				COMMIT_QUEUE,
				containsInAnyOrder(EXPECTED_COMMITTED_DATA_IN_BATCH_MODE.toArray()));
	}

	@Test
	public void writerAndGlobalCommitterExecuteInStreamingMode() throws Exception {
		final StreamExecutionEnvironment env = buildStreamEnv();
		final FiniteTestSource<Integer> source = new FiniteTestSource<>(SOURCE_DATA);

		env.addSource(source, IntegerTypeInfo.INT_TYPE_INFO)
				.sinkTo(TestSink
						.newBuilder()
						.setCommittableSerializer(TestSink.StringCommittableSerializer.INSTANCE)
						.setGlobalCommitter((Supplier<Queue<String>> & Serializable) () -> GLOBAL_COMMIT_QUEUE)
						.build());

		env.execute();
		assertThat(
				GLOBAL_COMMIT_QUEUE,
				containsInAnyOrder(EXPECTED_GLOBAL_COMMITTED_DATA_IN_STREAMING_MODE.toArray()));
	}

	@Test
	public void writerAndGlobalCommitterExecuteInBatchMode() throws Exception {
		final StreamExecutionEnvironment env = buildBatchEnv();

		env.fromCollection(SOURCE_DATA)
				.sinkTo(TestSink
						.newBuilder()
						.setCommittableSerializer(TestSink.StringCommittableSerializer.INSTANCE)
						.setGlobalCommitter((Supplier<Queue<String>> & Serializable) () -> GLOBAL_COMMIT_QUEUE)
						.build());
		env.execute();

		assertThat(
				GLOBAL_COMMIT_QUEUE,
				containsInAnyOrder(EXPECTED_GLOBAL_COMMITTED_DATA_IN_BATCH_MODE.toArray()));

	}

	private StreamExecutionEnvironment buildStreamEnv() {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final Configuration config = new Configuration();
		config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.STREAMING);
		env.enableCheckpointing(100);
		return env;
	}

	private StreamExecutionEnvironment buildBatchEnv() {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final Configuration config = new Configuration();
		config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
		env.configure(config, this.getClass().getClassLoader());
		return env;
	}
}
