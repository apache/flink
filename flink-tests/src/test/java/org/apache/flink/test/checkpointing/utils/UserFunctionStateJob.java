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

package org.apache.flink.test.checkpointing.utils;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.checkpointing.utils.SavepointUtil.SavepointCondition;
import org.apache.flink.test.checkpointing.utils.SavepointUtil.SavepointTestJob;
import org.apache.flink.util.Collector;

public class UserFunctionStateJob implements SavepointTestJob {

	@Override
	public JobGraph createJobGraph() {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		// we only test memory state backend yet
		env.setStateBackend(new MemoryStateBackend());
		env.enableCheckpointing(500);
		env.setParallelism(1);
		env.setMaxParallelism(1);

		// create source
		final DataStream<Tuple2<Long, Long>> source = env
			.addSource(new SourceFunction<Tuple2<Long, Long>>() {

				private volatile boolean isRunning = true;

				@Override
				public void run(SourceContext<Tuple2<Long, Long>> ctx) throws Exception {
					while (isRunning) {
						synchronized (ctx.getCheckpointLock()) {
							ctx.collect(new Tuple2<>(1L, 1L));
						}
					}
				}

				@Override
				public void cancel() {
					isRunning = false;
				}
			}).uid("CustomSourceFunction");

		// non-keyed operator state
		source.flatMap(new SumFlatMapperNonKeyedCheckpointed()).uid("SumFlatMapperNonKeyedCheckpointed").startNewChain().print();

		return env.getStreamGraph().getJobGraph();
	}

	@Override
	public SavepointCondition[] getSavepointCondition() {
		return new SavepointCondition[] {
			new SavepointCondition(SumFlatMapperNonKeyedCheckpointed.class, 0, 4)
		};
	}

	public static class SumFlatMapperNonKeyedCheckpointed extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>
			implements Checkpointed<Tuple2<Long, Long>> {

		private transient Tuple2<Long, Long> sum;

		@Override
		public void restoreState(Tuple2<Long, Long> state) throws Exception {
			sum = state;
		}

		@Override
		public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Long>> out) throws Exception {
			if (SavepointUtil.allowStateChange()) {
				if (sum == null) {
					sum = value;
					out.collect(sum);
				} else {
					sum.f1 += value.f1;
					out.collect(sum);
				}
			}

			SavepointUtil.triggerOrTestSavepoint(
				this,
				new Tuple2<>(value.f1, value.f1 * 4),
				sum);
		}

		@Override
		public Tuple2<Long, Long> snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			return sum;
		}
	}
}
