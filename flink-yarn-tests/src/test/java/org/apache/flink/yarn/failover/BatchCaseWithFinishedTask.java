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

package org.apache.flink.yarn.failover;

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator.Context;
import org.apache.flink.streaming.api.transformations.StreamTransformation;

import java.lang.reflect.Field;
import java.util.List;

/**
 * Testing job for {@link org.apache.flink.runtime.jobmaster.JobMaster} failover.
 * Covering batch case that have a finite source and a blocking sink, scheduling by
 * LAZY_FROM_SOURCES mode, with BLOCKING edges.
 */
public class BatchCaseWithFinishedTask {

	// *************************************************************************
	//     PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);
		final String casename = params.get("casename");

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setExecutionMode(ExecutionMode.BATCH);

		env.addSource(new FiniteSourceFunction())
			.setParallelism(2)
			.shuffle()
			.map(new RichMapFunction<Integer, Integer>() {
				@Override
				public Integer map(Integer value) throws Exception {
					synchronized (this) {
						wait();
					}
					return value;
				}
			})
			.setParallelism(2)
			.addSink(new DiscardingSink<>());

		Field field = env.getClass().getSuperclass().getDeclaredField("transformations");
		field.setAccessible(true);
		List<StreamTransformation<?>> transformations = (List<StreamTransformation<?>>) field.get(env);

		Context context = Context.buildBatchProperties(env);
		StreamGraph streamGraph = StreamGraphGenerator.generate(context, transformations);
		streamGraph.setJobName(casename);

		env.execute(streamGraph);
	}

	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************

	private static final class FiniteSourceFunction extends RichParallelSourceFunction<Integer> {
		FiniteSourceFunction() {
		}

		@Override
		public void run(SourceContext<Integer> ctx) {
			for (int i = 0; i < 1000; i++) {
				ctx.collect(0);
			}
		}

		@Override
		public void cancel() {
		}
	}
}
