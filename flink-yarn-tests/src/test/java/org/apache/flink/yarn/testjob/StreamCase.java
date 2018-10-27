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

package org.apache.flink.yarn.testjob;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * Testing job for {@link org.apache.flink.runtime.jobmaster.JobMaster} failover.
 * Covering stream case that have a infinite source and a sink, scheduling by
 * EAGER mode, with PIPELINED edges.
 */
public class StreamCase {

	// *************************************************************************
	//     PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.addSource(new InfiniteSourceFunction())
			.setParallelism(2)
			.shuffle()
			.addSink(new DiscardingSink<>())
			.setParallelism(2);

		env.execute("StreamCase");
	}

	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************

	private static final class InfiniteSourceFunction extends RichParallelSourceFunction<Integer> {
		private boolean running;

		InfiniteSourceFunction() {
			running = true;
		}

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			while (running) {
				synchronized (ctx.getCheckpointLock()) {
					ctx.collect(0);
				}

				Thread.sleep(5L);
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}
}
