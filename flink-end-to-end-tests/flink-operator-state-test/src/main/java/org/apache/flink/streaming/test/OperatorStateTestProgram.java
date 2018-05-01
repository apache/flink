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

package org.apache.flink.streaming.test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Collections;
import java.util.List;

/**
 * End to end test for operator state in a job.
 *
 * <p>Program to test a operator state within an operator, to count the number to
 * a new value is the old value + 1.
 *
 * <p>Program parameters:
 * -outputPath Sets the path to where the result data is written.
 */
public class OperatorStateTestProgram {
	public static void main(String[] args) throws Exception {
		ParameterTool params = ParameterTool.fromArgs(args);
		String outputPath = params.getRequired("outputPath");

		StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		sEnv.setParallelism(1);
		sEnv
			.addSource(new DataGenerator())
			.map(new CountUpMap())
			.writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE);

		sEnv.execute();
	}

	/**
	 * The map operator that record the number by using operator state.
	 */
	public static class CountUpMap implements MapFunction<Tuple1<String>, Tuple2<String, Integer>>,
		ListCheckpointed<Integer> {

		private int count;

		@Override
		public Tuple2<String, Integer> map(Tuple1<String> value) throws Exception {
			count++;
			return new Tuple2<>(value.f0, count);
		}

		@Override
		public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
			return Collections.singletonList(count);
		}

		@Override
		public void restoreState(List<Integer> state) throws Exception {
			for (Integer i : state) {
				count += i;
			}
		}
	}

	/**
	 * Data-generating source function.
	 */
	public static class DataGenerator implements SourceFunction<Tuple1<String>> {

		@Override
		public void run(SourceContext<Tuple1<String>> ctx) throws Exception {
			for (int i = 0; i < 1000; i++) {
				synchronized (ctx.getCheckpointLock()) {
					ctx.collect(Tuple1.of("Some payloads......"));
				}
			}
		}

		@Override
		public void cancel() {

		}
	}
}
