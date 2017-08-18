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

package org.apache.flink.test.runtime.entrypoint;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.FileMonitoringFunction;

import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;

/**
 * A program to generate a job graph for entrypoint testing purposes.
 *
 * <p>The dataflow is a simple streaming program that continuously monitors a (non-existent) directory.
 * Note that the job graph doesn't depend on any user code; it uses in-built Flink classes only.
 *
 * <p>Program arguments:
 *  --output [graph file] (default: 'job.graph')
 */
public class StreamingNoop {
	public static void main(String[] args) throws Exception {
		ParameterTool params = ParameterTool.fromArgs(args);

		// define the dataflow
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(2);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 1000));
		env.readFileStream("input/", 60000, FileMonitoringFunction.WatchType.ONLY_NEW_FILES)
			.addSink(new DiscardingSink<String>());

		// generate a job graph
		final JobGraph jobGraph = env.getStreamGraph().getJobGraph();
		File jobGraphFile = new File(params.get("output", "job.graph"));
		try (FileOutputStream output = new FileOutputStream(jobGraphFile);
			ObjectOutputStream obOutput = new ObjectOutputStream(output)){
			obOutput.writeObject(jobGraph);
		}
	}
}
