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

package org.apache.flink.connector.file.sink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.operators.StreamSource;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests the functionality of the {@link FileSink} in BATCH mode.
 */
@RunWith(Parameterized.class)
public class BatchExecutionFileSinkITCase extends FileSinkITBase {

	/**
	 * Creating the testing job graph in batch mode. The graph created is
	 * [Source] -> [Failover Map -> File Sink]. The Failover Map is introduced
	 * to ensure the failover would always restart the file writer so the data
	 * would be re-written.
	 */
	protected JobGraph createJobGraph(String path) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		Configuration config = new Configuration();
		config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
		env.configure(config, getClass().getClassLoader());

		if (triggerFailover) {
			env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, Time.milliseconds(100)));
		} else {
			env.setRestartStrategy(RestartStrategies.noRestart());
		}

		// Create a testing job with a bounded legacy source in a bit hacky way.
		StreamSource<Integer, ?> sourceOperator = new StreamSource<>(new BatchExecutionTestSource(NUM_RECORDS));
		DataStreamSource<Integer> source = new DataStreamSource<>(
				env,
				BasicTypeInfo.INT_TYPE_INFO,
				sourceOperator,
				true,
				"Source",
				Boundedness.BOUNDED);

		source
				.setParallelism(NUM_SOURCES)
				.rebalance()
				.map(new BatchExecutionOnceFailingMap(NUM_RECORDS, triggerFailover))
				.setParallelism(NUM_SINKS)
				.sinkTo(createFileSink(path))
				.setParallelism(NUM_SINKS);

		StreamGraph streamGraph = env.getStreamGraph();
		return streamGraph.getJobGraph();
	}

	//------------------------ Blocking mode user functions ----------------------------------

	/**
	 * A testing source that blocks until we see at least once successful checkpoint. We need this
	 * to ensure that our sink (which is also in the pipeline) has the chance to commit the output
	 * data.
	 */
	private static class BatchExecutionTestSource extends RichParallelSourceFunction<Integer> {

		private final int numberOfRecords;

		private volatile boolean isCanceled;

		public BatchExecutionTestSource(int numberOfRecords) {
			this.numberOfRecords = numberOfRecords;
		}

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			for (int i = 0; !isCanceled && i < numberOfRecords; ++i) {
				ctx.collect(i);
			}
		}

		@Override
		public void cancel() {
			isCanceled = true;
		}
	}

	/**
	 * A {@link RichMapFunction} that throws an exception to fail the job iff {@code
	 * triggerFailover} is {@code true} and when it is subtask 0 and we're in execution attempt 0.
	 */
	private static class BatchExecutionOnceFailingMap extends RichMapFunction<Integer, Integer> {

		private final int maxNumber;

		private final boolean triggerFailover;

		public BatchExecutionOnceFailingMap(int maxNumber, boolean triggerFailover) {
			this.maxNumber = maxNumber;
			this.triggerFailover = triggerFailover;
		}

		@Override
		public Integer map(Integer value) {
			if (triggerFailover &&
					getRuntimeContext().getIndexOfThisSubtask() == 0 &&
					getRuntimeContext().getAttemptNumber() == 0 &&
					value >= FAILOVER_RATIO * maxNumber) {
				throw new RuntimeException("Designated Failure");
			}

			return value;
		}
	}
}
