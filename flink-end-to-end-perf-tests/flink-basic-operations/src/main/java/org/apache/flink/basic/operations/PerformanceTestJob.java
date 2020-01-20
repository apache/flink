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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
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

	private enum  STREAMPARTITIONER{
		FORWARD("FORWARD"),
		RESCALE("RESCALE"),
		REBALANCE("REBALANCE"),
		SHUFFLE("SHUFFLE"),
		KEYBY("KEYBY"),
		BROADCAST("BROADCAST");

		private String name;
		private STREAMPARTITIONER(String name){
			this.name = name;
		}

		public String getName(){
			return this.name;
		}
	}

	private enum CHECKPOINTMODE{
		ATLEASEONCE("ATLEASEONCE"),
		EXACTLYONCE("EXACTLYONCE");

		private String name;
		private CHECKPOINTMODE(String name){
			this.name = name;
		}

		public String getName(){
			return this.name;
		}
	}

	private enum STATEBACKEND{
		ROCKSDB("ROCKSDB"),
		HEAP("HEAP");

		private String name;
		private STATEBACKEND(String name){
			this.name = name;
		}

		public String getName(){
			return this.name;
		}
	}

	private enum EXECUTIONMODE{
		PIPELINED("PIPELINED"),
		BATCH("BATCH");

		private String name;
		private EXECUTIONMODE(String name){
			this.name = name;
		}

		public String getName(){
			return this.name;
		}
	}

	private enum SCHEDULEMODE{
		EAGER("EAGER"),
		LAZY_FROM_SOURCES("LAZY_FROM_SOURCES");

		private String name;
		private SCHEDULEMODE(String name){
			this.name = name;
		}

		public String getName(){
			return this.name;
		}
	}

	private enum TOPOLOGYNAME{
		ONEINPUT("ONEINPUT"),
		TWOINPUTS("TWOINPUTS");

		private String name;
		private TOPOLOGYNAME(String name){
			this.name = name;
		}

		public String getName(){
			return this.name;
		}
	}

	private static final ConfigOption<String> JOBNAME = ConfigOptions.key("jobName")
		.defaultValue("PerformanceTestJob").withDescription("the name of test job");

	private static final ConfigOption<Integer>  PARALLELISM = ConfigOptions.key("parallelism")
		.defaultValue(1).withDescription("the parallelism of test job");
	private static final ConfigOption<String>  OUTPUTPATH = ConfigOptions.key("outputPath")
		.defaultValue("").withDescription("the output path of sink");
	private static final ConfigOption<String>  CHECKPOINT_MODE = ConfigOptions.key("checkPointMode")
		.defaultValue(CHECKPOINTMODE.ATLEASEONCE.toString()).withDescription("checkpointMode");
	private static final ConfigOption<Integer>  CHECKPOINT_TIMEOUT = ConfigOptions.key("checkPointTimeout")
		.defaultValue(60 * 15).withDescription("");
	private static final ConfigOption<Long>  MAX_COUNT = ConfigOptions.key("maxCount")
		.defaultValue(Long.MAX_VALUE).withDescription("the record num");
	private static final ConfigOption<Integer>  SEED = ConfigOptions.key("seed")
		.defaultValue(0).withDescription("seed of random to generate source data");
	private static final ConfigOption<Integer>  SLEEP_NUM = ConfigOptions.key("sleepNum")
		.defaultValue(0).withDescription("the interval of two records");
	private static final ConfigOption<String>  STREAM_PARTITIONER = ConfigOptions.key("streamPartitioner")
		.defaultValue("keyby").withDescription("");
	private static final ConfigOption<String>  SCHEDULE_MODE = ConfigOptions.key("scheduleMode")
		.defaultValue("EAGER").withDescription("");
	private static final ConfigOption<String>  EXECUTION_MODE = ConfigOptions.key("executionMode")
		.defaultValue("PIPELINED").withDescription("");
	private static final ConfigOption<Long>  CHECKPOINT_INTERVAL = ConfigOptions.key("checkpointInterval")
		.defaultValue(0L).withDescription("");
	private static final ConfigOption<String>  CHECKPOINT_PATH = ConfigOptions.key("checkpointPath")
		.defaultValue("").withDescription("");
	private static final ConfigOption<String>  STATE_BACKEND = ConfigOptions.key("stateBackend")
		.defaultValue("heap").withDescription("");
	private static final ConfigOption<Integer>  RECORD_SIZE = ConfigOptions.key("recordSize")
		.defaultValue(1024).withDescription("record size");
	private static final ConfigOption<String>  TOPOLOGY_NAME = ConfigOptions.key("topologyName")
		.defaultValue("oneInput").withDescription("The topology type ,only support oneInput or TwoInputs");

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
			WordCountData.getWords(recordValue.length(), seed)).setParallelism(parallism).name(sourceName);

		DataStream<Tuple2<String, String>> flapNode =
			sourceNode
				.flatMap((String value, Collector<Tuple2<String, String>> collector)
					-> collector.collect(new Tuple2<String, String>(value, recordValue)))
				.returns(Types.TUPLE(Types.STRING, Types.STRING))
				.setParallelism(parallism)
				.keyBy(0)
				.flatMap(new ValueStateFlatMap());

		DataStream<Tuple2<String, String>> outputNode = null;
		if (STREAMPARTITIONER.FORWARD.getName().equals(streamPartitioner)) {
			outputNode = flapNode.forward();
		} else if (STREAMPARTITIONER.RESCALE.getName().equals(streamPartitioner)) {
			outputNode = flapNode.rescale();
		} else if (STREAMPARTITIONER.REBALANCE.getName().equals(streamPartitioner)) {
			outputNode = flapNode.rebalance();
		} else if (STREAMPARTITIONER.SHUFFLE.getName().equals(streamPartitioner)) {
			outputNode = flapNode.shuffle();
		} else if (STREAMPARTITIONER.KEYBY.getName().equals(streamPartitioner)) {
			outputNode = flapNode.keyBy(0);
		} else if (STREAMPARTITIONER.BROADCAST.getName().equals(streamPartitioner)) {
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
		Configuration config = paramTools.getConfiguration();
		String jobName = config.getString(JOBNAME);
		int parallelism = config.getInteger(PARALLELISM);
		String outputPath = config.getString(OUTPUTPATH);
		String checkPointMode = config.getString(CHECKPOINT_MODE).toUpperCase();
		int checkPointTimeout = config.getInteger(CHECKPOINT_TIMEOUT);
		long maxCount = config.getLong(MAX_COUNT);
		int seed = config.getInteger(SEED);
		int sleepNum = config.getInteger(SLEEP_NUM);
		String streamPartitioner = config.getString(STREAM_PARTITIONER).toUpperCase();
		String scheduleMode = config.getString(SCHEDULE_MODE).toUpperCase();
		String executionMode = config.getString(EXECUTION_MODE).toUpperCase();
		long checkpointInterval = config.getLong(CHECKPOINT_INTERVAL);
		String checkpointPath = config.getString(CHECKPOINT_PATH);
		String stateBackend = config.getString(STATE_BACKEND).toUpperCase();
		int recordSize = config.getInteger(RECORD_SIZE);
		String topologyName = config.getString(TOPOLOGY_NAME).toUpperCase();

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getCheckpointConfig().setCheckpointTimeout(checkPointTimeout);
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		env.setParallelism(parallelism);
		env.setMaxParallelism(30720);

		PerformanceTestJob testJob = new PerformanceTestJob();
		if (TOPOLOGYNAME.ONEINPUT.getName().equals(topologyName)){
			DataStream<Tuple2<String, String>> output = null;
			output = testJob.generateGraph("oneInput", env, recordSize, maxCount, sleepNum, seed, parallelism,
				streamPartitioner,
				outputPath);
			if (!"".equals(outputPath)){
				output.writeAsText(outputPath + "/output", WriteMode.OVERWRITE).name("output").setParallelism(parallelism);
			}
		} else if (TOPOLOGYNAME.TWOINPUTS.getName().equals(topologyName)){
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
			if (CHECKPOINTMODE.ATLEASEONCE.getName().equals(checkPointMode)) {
				env.enableCheckpointing(checkpointInterval, CheckpointingMode.AT_LEAST_ONCE);
			} else {
				env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE);
			}
			if (STATEBACKEND.ROCKSDB.getName().equals(stateBackend)) {
				RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(checkpointPath, true);
				env.setStateBackend(rocksDbBackend);
			} else {
				FsStateBackend fsStateBackend = new FsStateBackend(new Path(checkpointPath));
				env.setStateBackend(fsStateBackend);
			}
			env.getCheckpointConfig().setCheckpointTimeout(checkPointTimeout);
		}

		if (EXECUTIONMODE.PIPELINED.getName().equals(executionMode)) {
			env.getConfig().setExecutionMode(ExecutionMode.PIPELINED);
		} else {
			env.getConfig().setExecutionMode(ExecutionMode.BATCH);
		}

		StreamGraph streamGraph = env.getStreamGraph();
		if (SCHEDULEMODE.EAGER.getName().equals(scheduleMode)) {
			streamGraph.getJobGraph().setScheduleMode(ScheduleMode.EAGER);
		} else {
			streamGraph.getJobGraph().setScheduleMode(ScheduleMode.LAZY_FROM_SOURCES);
		}
		streamGraph.setJobName(jobName);
		env.execute(streamGraph);
	}

}

