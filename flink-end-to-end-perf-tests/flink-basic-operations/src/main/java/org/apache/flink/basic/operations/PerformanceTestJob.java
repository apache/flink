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

package org.apache.flink.basic.operations;

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.basic.utils.FromElements;
import org.apache.flink.basic.utils.ValueStateFlatMap;
import org.apache.flink.basic.utils.WordCountData;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.Collector;

/**
 * End-to-end perf test for end-to-end perf test.
 *
 * <p>The sources are generated and bounded. The result is always constant.
 *
 * <p>Parameters:
 * --jobName job's name
 * --parallelism  source Node's parallelism
 * --checkPointMode  checkpointMode，atleastone or exactlyonce
 * --seed
 * --sleepNum
 * --streamPartitioner
 * --scheduleMode
 * --executionMode
 * --inputPath if source data in hdfs then input the inputPath
 * --checkpointInterval
 * --checkpointPath
 * --stateBackend
 * --checkpointTimeout
 */
public class PerformanceTestJob {
	private static byte[] size10B = new byte[10];
	private static String recordSize10B = new String(size10B);
	private static byte[] size100B = new byte[100];
	private static String recordSize100B = new String(size100B);
	private static byte[] size1KB = new byte[1024];
	private static String recordSize1KB = new String(size1KB);
	private static String recordValue;

	protected DataStream<Tuple2<String, String>> generateGraph(
			String sourceName,
			StreamExecutionEnvironment env,
			int recordSize,
			long maxCount,
			int sleepNum,
			int seed,
			int parallism,
			String streamPartitioner,
			String outputPath){
		if (recordSize == 10){
			recordValue = recordSize10B;
		} else if (recordSize == 100){
			recordValue = recordSize100B;
		} else {
			recordValue = recordSize1KB;
		}

		DataStream<String> sourceNode = FromElements.fromElements(env, maxCount, sleepNum,
			WordCountData.getWords(recordValue.length(), true, seed)).setParallelism(parallism).name(sourceName);

		DataStream<Tuple2<String, String>> flapNode =
			sourceNode
				.flatMap((String value, Collector<Tuple2<String, String>> collector)
					-> collector.collect(new Tuple2<String, String>(value, recordValue)))
				.returns(Types.TUPLE(Types.STRING, Types.STRING))
				.setParallelism(parallism)
				.keyBy(0)
				.flatMap(new ValueStateFlatMap());

		DataStream<Tuple2<String, String>> outputNode = null;

		if ("forward".equals(streamPartitioner)) {
			outputNode = flapNode.forward();
		} else if ("rescale".equals(streamPartitioner)) {
			outputNode = flapNode.rescale();
		} else if ("rebalance".equals(streamPartitioner)) {
			outputNode = flapNode.rebalance();
		} else if ("shuffle".equals(streamPartitioner)) {
			outputNode = flapNode.shuffle();
		} else if ("keyby".equals(streamPartitioner)) {
			outputNode = flapNode.keyBy(0);
		} else if ("broadcast".equals(streamPartitioner)) {
			outputNode = flapNode.broadcast();
		} else {
			throw new IllegalArgumentException("Argument streamPartitioner is illegal!");
		}
		return outputNode;

	}

	protected DataStream<Tuple3<String, String, String>> connector(
		DataStream<Tuple2<String, String>> output1,
		DataStream<Tuple2<String, String>> output2,
		int parallelism){
		DataStream<Tuple3<String, String, String>> output = output1.connect(output2).process(
			new CoProcessFunction<Tuple2<String, String>, Tuple2<String, String>, Tuple3<String, String, String>>() {
				@Override
				public void processElement1(Tuple2<String, String> value, Context ctx, Collector<Tuple3<String, String, String>> out) throws Exception {
					out.collect(new Tuple3<>(value.f0, value.f1, value.f1));
				}

				@Override
				public void processElement2(Tuple2<String, String> value, Context ctx, Collector<Tuple3<String, String, String>>out) throws Exception {
					processElement1(value, ctx, out);
				}

			}, new TupleTypeInfo<>(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO)).name("downStreamWithInputOrder").setParallelism(parallelism);
		return output;
	}

	public static void main(String[] args) throws Exception {

		ParameterTool paramTools = ParameterTool.fromArgs(args);
		String jobName = paramTools.get("jobName", "Basic PerformanceTestJob");
		int parallelism = paramTools.getInt("parallelism", 1);
		String outputPath = paramTools.get("outputPath", "");
		String checkPointMode = paramTools.get("checkPointMode", "AtLeastOnce").toLowerCase();
		int checkPointTimeout = paramTools.getInt("checkPointTimeout", 60 * 15);
		long maxCount = paramTools.getLong("maxCount", Long.MAX_VALUE);
		int seed = paramTools.getInt("seed", 0);
		int sleepNum = paramTools.getInt("sleepNum", 0);
		String streamPartitioner = paramTools.get("streamPartitioner", "KeyBy").toLowerCase();
		String scheduleMode = paramTools.get("scheduleMode", "EAGER").toLowerCase();
		String executionMode = paramTools.get("executionMode", "PIPELINED").toLowerCase();
		long checkpointInterval = paramTools.getLong("checkpointInterval", 0L);
		String checkpointPath = paramTools.get("checkpointPath", "/");
		String stateBackend = paramTools.get("stateBackend", "heap").toLowerCase();
		long checkpointTimeout = paramTools.getLong("checkpointTimeout", 15 * 1000L);
		int recordSize = paramTools.getInt("recordSize", 1024);
		String topologyName = paramTools.get("topologyName", "oneInput").toLowerCase();

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getCheckpointConfig().setCheckpointTimeout(checkPointTimeout);
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		env.setParallelism(parallelism);
		env.setMaxParallelism(30720);

		PerformanceTestJob testJob = new PerformanceTestJob();
		if ("oneinput".equals(topologyName)){
			DataStream<Tuple2<String, String>> output = null;
			output = testJob.generateGraph("oneInput", env, recordSize, maxCount, sleepNum, seed, parallelism,
				streamPartitioner,
				outputPath);
			if (!"".equals(outputPath)){
				output.writeAsText(outputPath + "/output", WriteMode.OVERWRITE).name("output").setParallelism(parallelism);
			}
		} else if ("twoinputs".equals(topologyName)){
			DataStream<Tuple3<String, String, String>> output = null;
			DataStream<Tuple2<String, String>> output1 = testJob.generateGraph("twoInputs1", env, recordSize, maxCount,
				sleepNum, seed, parallelism, streamPartitioner, outputPath);
			DataStream<Tuple2<String, String>> output2 = testJob.generateGraph("twoInputs2", env, recordSize, maxCount,
				sleepNum, seed, parallelism, streamPartitioner, outputPath);
			output = testJob.connector(output1, output2, parallelism);
			if (!"".equals(outputPath)){
				output.writeAsText(outputPath + "/output", WriteMode.OVERWRITE).name("output").setParallelism(parallelism);
			}
		}
		if (checkpointInterval > 0) {
			if ("atleastonce".equals(checkPointMode)) {
				env.enableCheckpointing(checkpointInterval, CheckpointingMode.AT_LEAST_ONCE);
			} else {
				env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE);
			}
			if ("rocksdb".equals(stateBackend)) {
				RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(checkpointPath, true);
				env.setStateBackend(rocksDbBackend);
			} else {
				FsStateBackend fsStateBackend = new FsStateBackend(new Path(checkpointPath));
				env.setStateBackend(fsStateBackend);
			}
			env.getCheckpointConfig().setCheckpointTimeout(checkpointTimeout);
		}

		if ("pipelined".equals(executionMode)) {
			env.getConfig().setExecutionMode(ExecutionMode.PIPELINED);
		} else {
			env.getConfig().setExecutionMode(ExecutionMode.BATCH);
		}

		StreamGraph streamGraph = env.getStreamGraph();
		if ("eager".equals(scheduleMode)) {
			streamGraph.getJobGraph().setScheduleMode(ScheduleMode.EAGER);
		} else {
			streamGraph.getJobGraph().setScheduleMode(ScheduleMode.LAZY_FROM_SOURCES);
		}
		streamGraph.setJobName(jobName);
		env.execute(streamGraph);
	}

}

