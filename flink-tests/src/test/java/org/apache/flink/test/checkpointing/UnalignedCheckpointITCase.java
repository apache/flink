/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.TestLogger;

import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertThat;

/**
 * Integration test for performing the unaligned checkpoint.
 */
public class UnalignedCheckpointITCase extends TestLogger {
	public static final String NUM_COMPLETED_CHECKPOINTS = "numCompletedCheckpoints";

	@Rule
	public final TemporaryFolder temp = new TemporaryFolder();

	@Rule
	public final Timeout timeout = Timeout.builder()
		.withTimeout(300, TimeUnit.SECONDS)
		.build();

	@Test
	public void shouldPerformUnalignedCheckpointOnNonparallelTopology() throws Exception {
		execute(1);
	}

	@Test
	public void shouldPerformUnalignedCheckpointOnLocalChannelsOnly() throws Exception {
		execute(2);
	}

	@Test
	public void shouldPerformUnalignedCheckpointOnRemoteChannels() throws Exception {
		execute(10);
	}

	@Test
	public void shouldPerformUnalignedCheckpointMassivelyParallel() throws Exception {
		execute(20);
	}

	private void execute(int paralellism) throws Exception {
		StreamExecutionEnvironment env = createEnv(paralellism);

		createDAG(env, 30);
		final JobExecutionResult executionResult = env.execute();

		assertThat(executionResult.<Long>getAccumulatorResult(NUM_COMPLETED_CHECKPOINTS) / paralellism,
			Matchers.greaterThanOrEqualTo(30L));
	}

	@Nonnull
	private LocalStreamEnvironment createEnv(final int parallelism) throws IOException {
		Configuration conf = new Configuration();
		final int numSlots = 3;
		conf.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, numSlots);
		conf.setFloat(TaskManagerOptions.NETWORK_MEMORY_FRACTION, .9f);
		conf.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, (parallelism + numSlots - 1) / numSlots);

		conf.setString(CheckpointingOptions.STATE_BACKEND, "filesystem");
		conf.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, temp.newFolder().toURI().toString());

		final LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(parallelism, conf);
		env.enableCheckpointing(100);
		env.getCheckpointConfig().enableUnalignedCheckpoints();
		return env;
	}

	private void createDAG(final StreamExecutionEnvironment env, final long minCheckpoints) {
		final SingleOutputStreamOperator<Integer> source = env.addSource(new IntegerSource(minCheckpoints));
		final SingleOutputStreamOperator<Integer> transform = source.shuffle().map(i -> 2 * i);
		transform.shuffle().addSink(new CountingSink<>());
	}

	private static class IntegerSource extends RichParallelSourceFunction<Integer> implements CheckpointListener {

		private final long minCheckpoints;
		private volatile boolean running = true;
		private LongCounter numCompletedCheckpoints = new LongCounter();

		public IntegerSource(final long minCheckpoints) {
			this.minCheckpoints = minCheckpoints;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			getRuntimeContext().addAccumulator(NUM_COMPLETED_CHECKPOINTS, numCompletedCheckpoints);
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) throws Exception {
			numCompletedCheckpoints.add(1);
		}

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			int counter = 0;
			while (running) {
				ctx.collect(counter++);

				if (numCompletedCheckpoints.getLocalValue() >= minCheckpoints) {
					cancel();
				}
			}

			// wait for all instances to finish, such that checkpoints are still processed
			Thread.sleep(1000);
		}

		@Override
		public void cancel() {
			running = false;
		}
	}

	private static class CountingSink<T> extends RichSinkFunction<T> {
		private LongCounter counter = new LongCounter();

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			getRuntimeContext().addAccumulator("outputs", counter);
		}

		@Override
		public void invoke(T value, Context context) throws Exception {
			counter.add(1);
			if (counter.getLocalValue() % 100 == 0) {
				Thread.sleep(1);
			}
		}
	}
}
