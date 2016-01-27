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

package org.apache.flink.test.manual;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import static org.junit.Assert.fail;

public class StreamingScalabilityAndLatency {
	
	public static void main(String[] args) throws Exception {
		if ((Runtime.getRuntime().maxMemory() >>> 20) < 5000) {
			throw new RuntimeException("This test program needs to run with at least 5GB of heap space.");
		}
		
		final int TASK_MANAGERS = 1;
		final int SLOTS_PER_TASK_MANAGER = 80;
		final int PARALLELISM = TASK_MANAGERS * SLOTS_PER_TASK_MANAGER;

		LocalFlinkMiniCluster cluster = null;

		try {
			Configuration config = new Configuration();
			config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, TASK_MANAGERS);
			config.setInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, 80);
			config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, SLOTS_PER_TASK_MANAGER);
			config.setInteger(ConfigConstants.TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY, 20000);

			config.setInteger("taskmanager.net.server.numThreads", 1);
			config.setInteger("taskmanager.net.client.numThreads", 1);

			cluster = new LocalFlinkMiniCluster(config, false);
			cluster.start();
			
			runPartitioningProgram(cluster.getLeaderRPCPort(), PARALLELISM);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			if (cluster != null) {
				cluster.shutdown();
			}
		}
	}
	
	private static void runPartitioningProgram(int jobManagerPort, int parallelism) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", jobManagerPort);
		env.setParallelism(parallelism);
		env.getConfig().enableObjectReuse();

		env.setBufferTimeout(5L);
		env.enableCheckpointing(1000, CheckpointingMode.AT_LEAST_ONCE);

		env
			.addSource(new TimeStampingSource())
			.map(new IdMapper<Tuple2<Long, Long>>())
			.partitionByHash(0)
			.addSink(new TimestampingSink());
		
		env.execute("Partitioning Program");
	}
	
	public static class TimeStampingSource implements ParallelSourceFunction<Tuple2<Long, Long>> {

		private static final long serialVersionUID = -151782334777482511L;

		private volatile boolean running = true;
		
		
		@Override
		public void run(SourceContext<Tuple2<Long, Long>> ctx) throws Exception {
			
			long num = 100;
			long counter = (long) (Math.random() * 4096);
			
			while (running) {
				if (num < 100) {
					num++;
					ctx.collect(new Tuple2<Long, Long>(counter++, 0L));
				}
				else {
					num = 0;
					ctx.collect(new Tuple2<Long, Long>(counter++, System.currentTimeMillis()));
				}

				Thread.sleep(1);
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}
	
	public static class TimestampingSink implements SinkFunction<Tuple2<Long, Long>> {

		private static final long serialVersionUID = 1876986644706201196L;

		private long maxLatency;
		private long count; 
		
		@Override
		public void invoke(Tuple2<Long, Long> value) {
			long ts = value.f1;
			if (ts != 0L) {
				long diff = System.currentTimeMillis() - ts;
				maxLatency = Math.max(diff, maxLatency);
			}
			
			count++;
			if (count == 5000) {
				System.out.println("Max latency: " + maxLatency);
				count = 0;
				maxLatency = 0;
			}
		}
	}

	public static class IdMapper<T> implements MapFunction<T, T> {

		private static final long serialVersionUID = -6543809409233225099L;

		@Override
		public T map(T value) {
			return value;
		}
	}
}
