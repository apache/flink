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

package org.apache.flink.streaming.examples.cellinfo;

import java.util.Random;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.co.RichCoMapFunction;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.util.Collector;

public class CellInfoLocal {

	private final static int CELL_COUNT = 10;
	private final static int LAST_MILLIS = 1000;
	private final static int PARALLELISM = 1;
	private final static int SOURCE_PARALLELISM = 1;
	private final static int QUERY_SLEEP_TIME = 1000;
	private final static int QUERY_COUNT = 10;
	private final static int INFO_SLEEP_TIME = 100;
	private final static int INFO_COUNT = 100;

	public final static class InfoSource implements SourceFunction<Tuple3<Integer, Long, Integer>> {
		private static final long serialVersionUID = 1L;

		private static Random rand = new Random();
		private Tuple3<Integer, Long, Integer> tuple = new Tuple3<Integer, Long, Integer>(0, 0L, 0);

		@Override
		public void invoke(Collector<Tuple3<Integer, Long, Integer>> out) throws Exception {
			for (int i = 0; i < INFO_COUNT; i++) {
				Thread.sleep(INFO_SLEEP_TIME);
				tuple.f0 = rand.nextInt(CELL_COUNT);
				tuple.f1 = System.currentTimeMillis();

				out.collect(tuple);
			}
		}
	}

	private final static class QuerySource implements
			SourceFunction<Tuple3<Integer, Long, Integer>> {
		private static final long serialVersionUID = 1L;

		private static Random rand = new Random();
		Tuple3<Integer, Long, Integer> tuple = new Tuple3<Integer, Long, Integer>(0, 0L,
				LAST_MILLIS);

		@Override
		public void invoke(Collector<Tuple3<Integer, Long, Integer>> collector) throws Exception {
			for (int i = 0; i < QUERY_COUNT; i++) {
				Thread.sleep(QUERY_SLEEP_TIME);
				tuple.f0 = rand.nextInt(CELL_COUNT);
				tuple.f1 = System.currentTimeMillis();
				collector.collect(tuple);
			}
		}
	}

	private final static class CellTask	extends
			RichCoMapFunction<Tuple3<Integer, Long, Integer>, Tuple3<Integer, Long, Integer>, String> {
		private static final long serialVersionUID = 1L;

		private WorkerEngineExact engine;
		private Integer cellID;
		private Long timeStamp;
		private Integer lastMillis;

		public CellTask() {
			engine = new WorkerEngineExact(CELL_COUNT, LAST_MILLIS, System.currentTimeMillis());
		}

		// INFO
		@Override
		public String map1(Tuple3<Integer, Long, Integer> value) {
			cellID = value.f0;
			timeStamp = value.f1;
			engine.put(cellID, timeStamp);
			return "INFO:\t" + cellID + " @ " + timeStamp;
		}

		// QUERY
		@Override
		public String map2(Tuple3<Integer, Long, Integer> value) {
			cellID = value.f0;
			timeStamp = value.f1;
			lastMillis = value.f2;
			return "QUERY:\t" + cellID + ": " + engine.get(timeStamp, lastMillis, cellID);
		}
	}

	// Example for connecting data streams
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(
				PARALLELISM).setBufferTimeout(100);

		DataStream<Tuple3<Integer, Long, Integer>> querySource = env.addSource(new QuerySource(),
				SOURCE_PARALLELISM).partitionBy(0);

		DataStream<String> stream = env.addSource(new InfoSource(), SOURCE_PARALLELISM)
				.partitionBy(0).connect(querySource).map(new CellTask());
		stream.print();

		env.execute();
	}
}
