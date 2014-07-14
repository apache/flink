/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.streaming.examples.cellinfo;

import java.util.Random;

import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.api.java.tuple.Tuple4;
import eu.stratosphere.streaming.api.DataStream;
import eu.stratosphere.streaming.api.SourceFunction;
import eu.stratosphere.streaming.api.StreamExecutionEnvironment;
import eu.stratosphere.util.Collector;

public class CellInfoLocal {

	private static Random rand = new Random();
	private final static int CELL_COUNT = 10;
	private final static int LAST_MILLIS = 1000;

	private final static class QuerySource extends
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

	private final static class InfoSource extends
			SourceFunction<Tuple4<Boolean, Integer, Long, Integer>> {
		private static final long serialVersionUID = 1L;

		private Tuple4<Boolean, Integer, Long, Integer> tuple = new Tuple4<Boolean, Integer, Long, Integer>(
				false, 0, 0L, 0);

		@Override
		public void invoke(Collector<Tuple4<Boolean, Integer, Long, Integer>> collector)
				throws Exception {
			for (int i = 0; i < 1000; i++) {
				Thread.sleep(100);

				tuple.f1 = rand.nextInt(CELL_COUNT);
				tuple.f2 = System.currentTimeMillis();

				collector.collect(tuple);
			}
		}
	}

	private final static class CellTask extends
			FlatMapFunction<Tuple4<Boolean, Integer, Long, Integer>, Tuple1<String>> {
		private static final long serialVersionUID = 1L;

		private WorkerEngineExact engine = new WorkerEngineExact(10, 500,
				System.currentTimeMillis());
		Integer cellID;
		Long timeStamp;
		Integer lastMillis;

		Tuple1<String> outTuple = new Tuple1<String>();

		@Override
		public void flatMap(Tuple4<Boolean, Integer, Long, Integer> value,
				Collector<Tuple1<String>> out) throws Exception {
			cellID = value.f1;
			timeStamp = value.f2;

			// QUERY
			if (value.f0) {
				lastMillis = value.f3;
				outTuple.f0 = "QUERY:\t"+cellID+ ": " + engine.get(timeStamp, lastMillis, cellID);
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

	// TODO add arguments
	public static void main(String[] args) {
		StreamExecutionEnvironment context = new StreamExecutionEnvironment();

		DataStream<Tuple4<Boolean, Integer, Long, Integer>> querySource = context
				.addSource(new QuerySource());

		DataStream<Tuple1<String>> stream = context.addSource(new InfoSource())
				.connectWith(querySource).partitionBy(1).flatMap(new CellTask()).addDummySink();

		context.execute();
	}
}
