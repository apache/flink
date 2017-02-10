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

package org.apache.flink.test.classloading.jar;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * This test is the same as the {@link CheckpointedStreamingProgram} but using the
 * old and deprecated {@link Checkpointed} interface. It stays here in order to
 * guarantee that although deprecated, the old Checkpointed interface is still supported.
 * This is necessary to not break user code.
 * */
public class LegacyCheckpointedStreamingProgram {

	private static final int CHECKPOINT_INTERVALL = 100;

	public static void main(String[] args) throws Exception {
		final String jarFile = args[0];
		final String host = args[1];
		final int port = Integer.parseInt(args[2]);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(host, port, jarFile);
		env.getConfig().disableSysoutLogging();
		env.enableCheckpointing(CHECKPOINT_INTERVALL);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 10000));
		env.disableOperatorChaining();

		DataStream<String> text = env.addSource(new SimpleStringGenerator());
		text.map(new StatefulMapper()).addSink(new NoOpSink());
		env.setParallelism(1);
		env.execute("Checkpointed Streaming Program");
	}


	// with Checkpointing
	public static class SimpleStringGenerator implements SourceFunction<String>, Checkpointed<Integer> {

		private static final long serialVersionUID = 3700033137820808611L;

		public boolean running = true;

		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			while(running) {
				Thread.sleep(1);
				ctx.collect("someString");
			}
		}

		@Override
		public void cancel() {
			running = false;
		}

		@Override
		public Integer snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			return null;
		}

		@Override
		public void restoreState(Integer state) {

		}
	}

	public static class StatefulMapper implements MapFunction<String, String>, Checkpointed<StatefulMapper>, CheckpointListener {

		private static final long serialVersionUID = 2703630582894634440L;

		private String someState;
		private boolean atLeastOneSnapshotComplete = false;
		private boolean restored = false;

		@Override
		public StatefulMapper snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			return this;
		}

		@Override
		public void restoreState(StatefulMapper state) {
			restored = true;
			this.someState = state.someState;
			this.atLeastOneSnapshotComplete = state.atLeastOneSnapshotComplete;
		}

		@Override
		public String map(String value) throws Exception {
			if(!atLeastOneSnapshotComplete) {
				// throttle consumption by the checkpoint interval until we have one snapshot.
				Thread.sleep(CHECKPOINT_INTERVALL);
			}
			if(atLeastOneSnapshotComplete && !restored) {
				throw new RuntimeException("Intended failure, to trigger restore");
			}
			if(restored) {
				throw new SuccessException();
				//throw new RuntimeException("All good");
			}
			someState = value; // update our state
			return value;
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) throws Exception {
			atLeastOneSnapshotComplete = true;
		}
	}
	// --------------------------------------------------------------------------------------------

	/**
	 * We intentionally use a user specified failure exception
	 */
	public static class SuccessException extends Exception {

		private static final long serialVersionUID = 7073311460437532086L;
	}

	public static class NoOpSink implements SinkFunction<String> {
		private static final long serialVersionUID = 2381410324190818620L;

		@Override
		public void invoke(String value) throws Exception {
		}
	}
}
