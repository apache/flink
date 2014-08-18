/**
 *
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
 *
 */

package org.apache.flink.streaming.examples.cellinfo;

import java.util.Random;

import org.apache.flink.api.java.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.util.Collector;

public class CellInfoLocal {

	private static Random rand = new Random();
	private final static int CELL_COUNT = 10;
	private final static int LAST_MILLIS = 1000;
	private final static int PARALLELISM = 1;
	private final static int SOURCE_PARALLELISM = 1;

	private final static class QuerySource implements
			SourceFunction<Tuple4<Boolean, Integer, Long, Integer>> {
		private static final long serialVersionUID = 1L;

		Tuple4<Boolean, Integer, Long, Integer> tuple = new Tuple4<Boolean, Integer, Long, Integer>(
				true, 0, 0L, LAST_MILLIS);

		@Override
		public void invoke(Collector<Tuple4<Boolean, Integer, Long, Integer>> collector)
				throws Exception {
			for (int i = 0; i < 100; i++) {
				Thread.sleep(1000);
				tuple.f1 = rand.nextInt(CELL_COUNT);
				tuple.f2 = System.currentTimeMillis();
				collector.collect(tuple);
			}
		}
	}

	public final static class InfoSource implements
			SourceFunction<Tuple4<Boolean, Integer, Long, Integer>> {
		private static final long serialVersionUID = 1L;

		private Tuple4<Boolean, Integer, Long, Integer> tuple = new Tuple4<Boolean, Integer, Long, Integer>(
				false, 0, 0L, 0);

		@Override
		public void invoke(Collector<Tuple4<Boolean, Integer, Long, Integer>> out) throws Exception {
			for (int i = 0; i < 1000; i++) {
				Thread.sleep(100);

				tuple.f1 = rand.nextInt(CELL_COUNT);
				tuple.f2 = System.currentTimeMillis();

				out.collect(tuple);
			}
		}
	}

	private final static class CellTask extends
			RichFlatMapFunction<Tuple4<Boolean, Integer, Long, Integer>, Tuple1<String>> {
		private static final long serialVersionUID = 1L;

		private WorkerEngineExact engine = new WorkerEngineExact(10, 500,
				System.currentTimeMillis());
		Integer cellID;
		Long timeStamp;
		Integer lastMillis;

		Tuple1<String> outTuple = new Tuple1<String>();

		// write information to String tuple based on the input tuple
		@Override
		public void flatMap(Tuple4<Boolean, Integer, Long, Integer> value,
				Collector<Tuple1<String>> out) throws Exception {
			cellID = value.f1;
			timeStamp = value.f2;

			// QUERY
			if (value.f0) {
				lastMillis = value.f3;
				outTuple.f0 = "QUERY:\t" + cellID + ": "
						+ engine.get(timeStamp, lastMillis, cellID);
				out.collect(outTuple);
			}
			// INFO
			else {
				engine.put(cellID, timeStamp);
				outTuple.f0 = "INFO:\t" + cellID + " @ " + timeStamp;
				out.collect(outTuple);
			}
		}
	}

	// In this example two different source then connect the two stream and
	// apply a function for the connected stream
	// TODO add arguments
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.createLocalEnvironment(PARALLELISM);

		DataStream<Tuple4<Boolean, Integer, Long, Integer>> querySource = env.addSource(
				new QuerySource(), SOURCE_PARALLELISM);

		DataStream<Tuple1<String>> stream = env.addSource(new InfoSource(), SOURCE_PARALLELISM)
				.merge(querySource).partitionBy(1).flatMap(new CellTask());
		stream.print();

		env.execute();
	}
}
