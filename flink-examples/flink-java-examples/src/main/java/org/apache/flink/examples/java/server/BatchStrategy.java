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

package org.apache.flink.examples.java.server;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.server.Parameter;
import org.apache.flink.api.common.server.Update;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.configuration.Configuration;

import java.util.Collection;

/**
 * Shows how to use the batch update strategy for Parameter Server
 * 
 */
@SuppressWarnings("serial")
public class BatchStrategy {
	
	// *************************************************************************
	//     PROGRAM
	// *************************************************************************
	
	public static void main(String[] args) throws Exception {
		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.generateSequence(1,4).setParallelism(4).map(new RichMapFunction<Long, Long>() {
			public void open(Configuration parameters) throws Exception {
				getRuntimeContext().registerBatch("KEY", new TestParameter(0L));
				// let's wait for everyone to get registered
				Thread.sleep(5000);
			}

			public Long map(Long value) throws Exception {
				for (int i = 0; i < 5; i++) {
					getRuntimeContext().updateParameter("KEY", new TestUpdate(value, i + 1));

					// sequential consistency
					// every batch adds 10 to the global sum.
					if ((i + 1) * 10 != ((TestParameter) getRuntimeContext().fetchParameter("KEY")).value) {
						throw new RuntimeException("Invalid results");
					}
				}
				return value;
			}
		}).output(new DiscardingOutputFormat<Long>());
		env.execute();
	}

	private static class TestParameter extends Parameter{
		private long value;

		public TestParameter(long value){
			this.value = value;
		}

		@Override
		public void update(Update update){
			this.value += ((TestUpdate) update).value;
		}

		@Override
		public void reduce(Collection<Update> list){
			for(Update u: list){
				this.value += ((TestUpdate) u).value;
			}
		}
	}

	private static class TestUpdate implements Update{
		private long value;
		private int clock;

		public TestUpdate(long value, int clock){
			this.value = value;
			this.clock = clock;
		}

		public int getClock(){
			return this.clock;
		}
	}
}
