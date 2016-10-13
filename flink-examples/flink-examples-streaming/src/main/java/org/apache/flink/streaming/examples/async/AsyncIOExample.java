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
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.async.AsyncCollector;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Example to illustrates how to use {@link org.apache.flink.streaming.api.functions.async.AsyncFunction}
 */
public class AsyncIOExample {

	/**
	 * A checkpointed source.
	 */
	private static class SimpleSource implements SourceFunction<Integer>, Checkpointed<Integer> {
		private static final long serialVersionUID = 1L;

		private volatile boolean isRunning = true;
		private int counter = 0;
		private int start = 0;

		@Override
		public void restoreState(Integer state) throws Exception {
			this.start = state;
		}

		@Override
		public Integer snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			return start;
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
					Thread.sleep(10);
				}
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}


	public static void main(String[] args) throws Exception {

		// obtain execution environment and set setBufferTimeout to 1 to enable
		// continuous flushing of the output buffers (lowest latency)
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
			.setBufferTimeout(1);

		// configurations for the job
		String statePath = args[0];
		String cpMode = args[1];
		int maxCount = Integer.valueOf(args[2]);
		final int sleepFactor = Integer.valueOf(args[3]);
		final float failRatio = Float.valueOf(args[4]);
		String mode = args[5];
		int taskNum = Integer.valueOf(args[6]);
		String timeType = args[7];

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

		// create input stream of integer pairs
		DataStream<Integer> inputStream = env.addSource(new SimpleSource(maxCount));

		// create async function, which will *wait* for a while to simulate the process of async i/o
		AsyncFunction<Integer, String> function = new RichAsyncFunction<Integer, String>() {
			transient ExecutorService executorService;

			@Override
			public void asyncInvoke(final Integer input, final AsyncCollector<Integer, String> collector) throws Exception {
				this.executorService.submit(new Runnable() {
					@Override
					public void run() {
						// wait for while to simulate async operation here
						int sleep = (int) (new Random().nextFloat() * sleepFactor);
						try {
							Thread.sleep(sleep);
							List<String> ret = new ArrayList<>();
							ret.add("key-"+(input%10));
							if (new Random().nextFloat() > failRatio) {
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

			private void readObject(java.io.ObjectInputStream in)
				throws IOException, ClassNotFoundException {
				in.defaultReadObject();
				executorService = Executors.newFixedThreadPool(30);
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



