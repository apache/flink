/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.datastream;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;

import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.util.CollectionUtil.iteratorToList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

/**
 * Integration test for {@link RuntimeExecutionMode#BATCH} execution on the DataStream API.
 *
 * <p>We use a {@link MiniClusterWithClientResource} with a single TaskManager with 1 slot to
 * verify that programs in BATCH execution mode can be executed in stages.
 */
public class DataStreamBatchExecutionITCase {
	private static final int DEFAULT_PARALLELISM = 1;

	@ClassRule
	public static MiniClusterWithClientResource miniClusterResource = new MiniClusterWithClientResource(
			new MiniClusterResourceConfiguration.Builder()
					.setNumberTaskManagers(1)
					.setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
					.build());

	/**
	 * We induce a failure in the last mapper. In BATCH execution mode the part of the pipeline
	 * before the key-by should not be re-executed. Only the part after that will restart. We check
	 * that by suffixing the attempt number to records and asserting the correct number.
	 */
	@Test
	public void batchFailoverWithKeyByBarrier() throws Exception {

		final StreamExecutionEnvironment env = getExecutionEnvironment();

		DataStreamSource<String> source = env.fromElements("foo", "bar");

		SingleOutputStreamOperator<String> mapped = source
				.map(new SuffixAttemptId("a"))
				.map(new SuffixAttemptId("b"))
				.keyBy(in -> in)
				.map(new SuffixAttemptId("c"))
				.map(new OnceFailingMapper("d"));

		try (CloseableIterator<String> result = mapped.executeAndCollect()) {

			// only the operators after the key-by "barrier" are restarted and will have the
			// "attempt 1" suffix
			assertThat(
					iteratorToList(result),
					containsInAnyOrder("foo-a0-b0-c1-d1", "bar-a0-b0-c1-d1"));
		}
	}

	/**
	 * We induce a failure in the last mapper. In BATCH execution mode the part of the pipeline
	 * before the rebalance should not be re-executed. Only the part after that will restart. We
	 * check that by suffixing the attempt number to records and asserting the correct number.
	 */
	@Test
	public void batchFailoverWithRebalanceBarrier() throws Exception {

		final StreamExecutionEnvironment env = getExecutionEnvironment();

		DataStreamSource<String> source = env.fromElements("foo", "bar");

		SingleOutputStreamOperator<String> mapped = source
				.map(new SuffixAttemptId("a"))
				.map(new SuffixAttemptId("b"))
				.rebalance()
				.map(new SuffixAttemptId("c"))
				.map(new OnceFailingMapper("d"));

		try (CloseableIterator<String> result = mapped.executeAndCollect()) {

			// only the operators after the rebalance "barrier" are restarted and will have the
			// "attempt 1" suffix
			assertThat(
					iteratorToList(result),
					containsInAnyOrder("foo-a0-b0-c1-d1", "bar-a0-b0-c1-d1"));
		}
	}

	/**
	 * We induce a failure in the last mapper. In BATCH execution mode the part of the pipeline
	 * before the rescale should not be re-executed. Only the part after that will restart. We
	 * check that by suffixing the attempt number to records and asserting the correct number.
	 */
	@Test
	public void batchFailoverWithRescaleBarrier() throws Exception {

		final StreamExecutionEnvironment env = getExecutionEnvironment();

		DataStreamSource<String> source = env.fromElements("foo", "bar");
		env.setParallelism(1);

		SingleOutputStreamOperator<String> mapped = source
			.map(new SuffixAttemptId("a"))
			.map(new SuffixAttemptId("b"))
			.rescale()
			.map(new SuffixAttemptId("c")).setParallelism(2)
			.map(new OnceFailingMapper("d")).setParallelism(2);

		try (CloseableIterator<String> result = mapped.executeAndCollect()) {

			// only the operators after the rescale "barrier" are restarted and will have the
			// "attempt 1" suffix
			assertThat(
				iteratorToList(result),
				containsInAnyOrder("foo-a0-b0-c1-d1", "bar-a0-b0-c1-d1"));
		}
	}

	@Test
	public void batchReduceSingleResultPerKey() throws Exception {
		StreamExecutionEnvironment env = getExecutionEnvironment();
		DataStreamSource<Long> numbers = env
			.fromSequence(0, 10);

		// send all records into a single reducer
		KeyedStream<Long, Long> stream = numbers.keyBy(i -> i % 2);
		DataStream<Long> sums = stream.reduce(Long::sum);

		try (CloseableIterator<Long> sumsIterator = sums.executeAndCollect()) {
			List<Long> results = CollectionUtil.iteratorToList(sumsIterator);
			assertThat(results, equalTo(Arrays.asList(
				30L, 25L
			)));
		}
	}

	@Test
	public void batchSumSingleResultPerKey() throws Exception {
		StreamExecutionEnvironment env = getExecutionEnvironment();
		DataStreamSource<Long> numbers = env
			.fromSequence(0, 10);

		// send all records into a single reducer
		KeyedStream<Long, Long> stream = numbers.keyBy(i -> i % 2);
		DataStream<Long> sums = stream.sum(0);

		try (CloseableIterator<Long> sumsIterator = sums.executeAndCollect()) {
			List<Long> results = CollectionUtil.iteratorToList(sumsIterator);
			assertThat(results, equalTo(Arrays.asList(
				30L, 25L
			)));
		}
	}

	private StreamExecutionEnvironment getExecutionEnvironment() {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setRuntimeMode(RuntimeExecutionMode.BATCH);
		env.setParallelism(1);

		// trick the collecting sink into working even in the face of failures 🙏
		env.enableCheckpointing(42);

		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.milliseconds(1)));

		return env;
	}


	/** Adds the attempt number as a suffix. */
	public static class SuffixAttemptId extends RichMapFunction<String, String> {
		private final String suffix;

		public SuffixAttemptId(String suffix) {
			this.suffix = suffix;
		}

		@Override
		public String map(String value) {
			return value + "-" + suffix + getRuntimeContext().getAttemptNumber();
		}
	}

	/**
	 * Adds the attempt number as a suffix.
	 *
	 * <p>Also fails by throwing an exception on the first attempt.
	 */
	public static class OnceFailingMapper extends RichMapFunction<String, String> {
		private final String suffix;

		public OnceFailingMapper(String suffix) {
			this.suffix = suffix;
		}

		@Override
		public String map(String value) throws Exception {
			if (getRuntimeContext().getAttemptNumber() <= 0) {
				throw new RuntimeException("FAILING");
			}
			return value + "-" + suffix + getRuntimeContext().getAttemptNumber();
		}
	}
}
