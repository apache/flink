/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.async;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.async.AsyncCollector;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Example to illustrates how to use {@link org.apache.flink.streaming.api.functions.async.AsyncFunction}
 */
public class AsyncIOExample {

	/**
	 * A checkpointed source.
	 */
	private static class SimpleSource implements SourceFunction<Integer>, ListCheckpointed<Integer> {
		private static final long serialVersionUID = 1L;

		private volatile boolean isRunning = true;
		private int counter = 0;
		private int start = 0;

		@Override
		public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
			return Collections.singletonList(start);
		}

		@Override
		public void restoreState(List<Integer> state) throws Exception {
			for (Integer i : state)
				this.start = i;
		}

		public SimpleSource(int maxNum) {
			this.counter = maxNum;
		}

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			while (start < counter && isRunning) {
				synchronized (ctx.getCheckpointLock()) {
					ctx.collect(start);
					++start;
				}
				Thread.sleep(10);
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}

	private static void printUsage() {
		System.out.println("To customize example, use: AsyncIOExample [--fsStatePath <path to fs state>] " +
			"[--checkpointMode <exactly_once or at_least_once>] [--maxCount <max number of input from source>] " +
			"[--sleepFactor <interval to sleep for each stream element>] [--failRatio <possibility to throw exception>] " +
			"[--waitMode <ordered or unordered>] [--waitOperatorParallelism <parallelism for async wait operator>] " +
			"[--eventType <EventTime or IngestionTime>]");
	}

	public static void main(String[] args) throws Exception {

		// obtain execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// parse parameters
		final ParameterTool params = ParameterTool.fromArgs(args);

		// check the configuration for the job
		final String statePath = params.getRequired("fsStatePath");
		final String cpMode = params.get("checkpointMode", "exactly_once");
		final int maxCount = params.getInt("maxCount", 100000);
		final int sleepFactor = params.getInt("sleepFactor", 100);
		final float failRatio = params.getFloat("failRatio", 0.001f);
		final String mode = params.get("waitMode", "ordered");
		final int taskNum =  params.getInt("waitOperatorParallelism", 1);
		final String timeType = params.get("eventType", "EventTime");

		System.out.println("Job configuration\n"
			+"\tFS state path="+statePath+"\n"
			+"\tCheckpoint mode="+cpMode+"\n"
			+"\tMax count of input from source="+maxCount+"\n"
			+"\tSleep factor="+sleepFactor+"\n"
			+"\tFail ratio="+failRatio+"\n"
			+"\tWaiting mode="+mode+"\n"
			+"\tParallelism for async wait operator="+taskNum+"\n"
			+"\tEvent type="+timeType);

		printUsage();

		// setup state and checkpoint mode
		env.setStateBackend(new FsStateBackend(statePath));
		if (cpMode.equals("exactly_once")) {
			env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
		}
		else {
			env.enableCheckpointing(1000, CheckpointingMode.AT_LEAST_ONCE);
			env.disableOperatorChaining();
		}

		// enable watermark or not
		if (timeType.equals("EventTime")) {
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		}
		else if (timeType.equals("IngestionTime")) {
			env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		}

		// create input stream of an single integer
		DataStream<Integer> inputStream = env.addSource(new SimpleSource(maxCount));

		// create async function, which will *wait* for a while to simulate the process of async i/o
		AsyncFunction<Integer, String> function = new RichAsyncFunction<Integer, String>() {
			transient ExecutorService executorService;
			transient Random random;

			@Override
			public void open(Configuration parameters) throws Exception {
				super.open(parameters);
				executorService = Executors.newFixedThreadPool(30);
				random = new Random();
			}

			@Override
			public void close() throws Exception {
				super.close();
				executorService.shutdown();
				executorService.awaitTermination(sleepFactor*20, TimeUnit.MILLISECONDS);
			}

			@Override
			public void asyncInvoke(final Integer input, final AsyncCollector<Integer, String> collector) throws Exception {
				this.executorService.submit(new Runnable() {
					@Override
					public void run() {
						// wait for while to simulate async operation here
						int sleep = (int) (random.nextFloat() * sleepFactor);
						try {
							Thread.sleep(sleep);
							List<String> ret = new ArrayList<>();
							ret.add("key-"+(input%10));
							if (random.nextFloat() < failRatio) {
								collector.collect(new Exception("wahahahaha..."));
							}
							else {
								collector.collect(ret);
							}
						}
						catch (InterruptedException e) {
							collector.collect(new ArrayList<String>(0));
						}
					}
				});
			}
		};

		// add async operator to streaming job
		DataStream<String> result;
		if (mode.equals("ordered")) {
			result = AsyncDataStream.orderedWait(inputStream, function, 20).setParallelism(taskNum);
		}
		else {
			result = AsyncDataStream.unorderedWait(inputStream, function, 20).setParallelism(taskNum);
		}

		// add a reduce to get the sum of each keys.
		result.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
			@Override
			public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
				out.collect(new Tuple2<>(value, 1));
			}
		}).keyBy(0).sum(1).print();

		// execute the program
		env.execute("Async I/O Example");
	}
}



