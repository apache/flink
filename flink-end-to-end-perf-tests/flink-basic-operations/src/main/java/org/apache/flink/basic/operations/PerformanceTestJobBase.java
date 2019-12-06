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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.basic.utils.FromElements;
import org.apache.flink.basic.utils.Params;
import org.apache.flink.basic.utils.ValueStateFlatMap;
import org.apache.flink.basic.utils.WordCountData;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
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
public class PerformanceTestJobBase {
	public StreamExecutionEnvironment  env;
	public Params params  = new Params();
	private static byte[] size10B = new byte[10];
	private static String recordSize10B = new String(size10B);
	private static byte[] size100B = new byte[100];
	private static String recordSize100B = new String(size100B);
	private static byte[] size1KB = new byte[1024];
	private static String recordSize1KB = new String(size1KB);
	private static String recordValue;

	public Params initParams(String[] args){
		ParameterTool paramTools = ParameterTool.fromArgs(args);
		params.initEnv(paramTools);
		return params;

	}

	public void setEnv(){
		env = getEnv();
		env.getCheckpointConfig().setCheckpointTimeout(params.getCheckpointTimeout());
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		env.setParallelism(params.getParallelism());
		env.setMaxParallelism(30720);
	}

	public StreamExecutionEnvironment getEnv(){
		return StreamExecutionEnvironment.getExecutionEnvironment();
	}

	public void setStateBackend() throws Exception{
		long checkpointInterval  = params.getCheckpointInterval();
		String checkpointMode = params.getCheckPointMode().toLowerCase();
		String statebackend = params.getStateBackend().toLowerCase();
		String checkpointPath  = params.getCheckpointPath();
		long checkpointTimeout = params.getCheckpointTimeout();
		if (checkpointInterval > 0) {
			if ("atleastonce".equals(checkpointMode)) {
				env.enableCheckpointing(checkpointInterval, CheckpointingMode.AT_LEAST_ONCE);
			} else {
				env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE);
			}
			if ("rocksdb".equals(statebackend)) {
				RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(checkpointPath, true);
				env.setStateBackend(rocksDbBackend);
			} else {
				FsStateBackend fsStateBackend = new FsStateBackend(new Path(checkpointPath));
				env.setStateBackend(fsStateBackend);
			}
			env.getCheckpointConfig().setCheckpointTimeout(checkpointTimeout);
		}

	}

	public DataStream<Tuple2<String, String>> setGraph(String sourceName){
		int recordSize = params.getRecordSize();
		if (recordSize == 10){
			recordValue = recordSize10B;
		} else if (recordSize == 100){
			recordValue = recordSize100B;
		} else {
			recordValue = recordSize1KB;
		}

		DataStream<String> sourceNode = FromElements.fromElements(env, params.getMaxCount(), params.getSleepNum(),
			WordCountData.getWords(recordValue.length(), true, params.getSeed())).setParallelism(params.getParallelism()).name(sourceName);

		//DataStream<Tuple2<String, String>> flapNode = sourceNode.flatMap(
		//	new FlatMapFunction<String, Tuple2<String, String>>() {
		//		@Override
		//		public void flatMap(String value, Collector<Tuple2<String, String>> collector) throws Exception {
		//			collector.collect(new Tuple2<>(value, recordValue));
		//		}
		//	}
		//).setParallelism(params.getParallelism()).keyBy(0).flatMap(new ValueStateFlatMap());
		DataStream<Tuple2<String, String>> flapNode = sourceNode.flatMap((String value, Collector<Tuple2<String,
			String>> collector) -> collector.collect(new Tuple2<>(value, recordValue))).setParallelism(params.getParallelism()).keyBy(0).flatMap(new ValueStateFlatMap());
		//	new FlatMapFunction<String, Tuple2<String, String>>() {
		//		@Override
		//		public void flatMap(String value, Collector<Tuple2<String, String>> collector) throws Exception {
		//			collector.collect(new Tuple2<>(value, recordValue));
		//		}
		//	}
		//).setParallelism(params.getParallelism()).keyBy(0).flatMap(new ValueStateFlatMap());
		DataStream<Tuple2<String, String>> outputNode = null;
		String streamPartitioner = params.getStreamPartitioner().toLowerCase();

		if ("forward".equals(streamPartitioner)) {
			if (params.getParallelism() == params.getParallelism()) {
				outputNode = flapNode.forward();
			} else {
				outputNode = flapNode;
			}

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
		String outputPath = params.getOutputPath();
		if (!"".equals(outputPath)){
			outputNode.writeAsCsv(outputPath);
		}
		return outputNode;

	}

	public void setExecuteMode(){
		String executeMode = params.getExecutionMode().toLowerCase();
		if ("pipelined".equals(executeMode)) {
			env.getConfig().setExecutionMode(ExecutionMode.PIPELINED);
		} else {
			env.getConfig().setExecutionMode(ExecutionMode.BATCH);
		}
	}

	public StreamGraph setScheduleMode(){
		String schedulerMode  = params.getScheduleMode().toLowerCase();
		StreamGraph streamGraph = env.getStreamGraph();
		if ("eager".equals(schedulerMode)) {
			streamGraph.getJobGraph().setScheduleMode(ScheduleMode.EAGER);
		} else {
			streamGraph.getJobGraph().setScheduleMode(ScheduleMode.LAZY_FROM_SOURCES);
		}
		return streamGraph;
	}

	public void run()throws Exception {
		StreamGraph streamGraph = setScheduleMode();
		streamGraph.setJobName(params.getJobName());
		env.execute(params.getJobName());
	}

	public void connector(DataStream<Tuple2<String, String>> output1, DataStream<Tuple2<String, String>> output2){
		output1.connect(output2).process(
			new CoProcessFunction<Tuple2<String, String>, Tuple2<String, String>, Tuple3<String, String, String>>() {
				@Override
				public void processElement1(Tuple2<String, String> value, Context ctx, Collector<Tuple3<String, String, String>> out) throws Exception {
					out.collect(new Tuple3<>(value.f0, value.f1, value.f1));
				}

				@Override
				public void processElement2(Tuple2<String, String> value, Context ctx, Collector<Tuple3<String, String, String>> out) throws Exception {
					processElement1(value, ctx, out);
				}

			}, new TupleTypeInfo<>(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO)).name("downStreamWithInputOrder").setParallelism(params.getParallelism());
	}

}

