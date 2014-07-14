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

package eu.stratosphere.streaming.api;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.util.Collector;

public class MapTest {

	public static final class MySource extends SourceFunction<Tuple1<Integer>> {

		@Override
		public void invoke(Collector<Tuple1<Integer>> collector) throws Exception {
			for (int i = 0; i < 10; i++) {
				System.out.println("source "+i);
				collector.collect(new Tuple1<Integer>(i));
			}
		}
	}

	public static final class MyFieldsSource extends SourceFunction<Tuple1<Integer>> {

		@Override
		public void invoke(Collector<Tuple1<Integer>> collector) throws Exception {
			for (int i = 0; i < MAXSOURCE; i++) {
				collector.collect(new Tuple1<Integer>(5));
			}
		}
	}
	
	public static final class MyDiffFieldsSource extends SourceFunction<Tuple1<Integer>> {

		@Override
		public void invoke(Collector<Tuple1<Integer>> collector) throws Exception {
			for (int i = 0; i < 9; i++) {
				collector.collect(new Tuple1<Integer>(i));
			}
		}
	}

	public static final class MyMap extends MapFunction<Tuple1<Integer>, Tuple1<Integer>> {

		@Override
		public Tuple1<Integer> map(Tuple1<Integer> value) throws Exception {
			System.out.println("mymap "+map);
			map++;
			return new Tuple1<Integer>(value.f0 * value.f0);
		}
	}

	public static final class MyFieldsMap extends MapFunction<Tuple1<Integer>, Tuple1<Integer>> {

		private int counter = 0;

		@Override
		public Tuple1<Integer> map(Tuple1<Integer> value) throws Exception {
			counter++;

			if (counter == MAXSOURCE)
				allInOne = true;
			return new Tuple1<Integer>(value.f0 * value.f0);
		}
	}
	
	public static final class MyDiffFieldsMap extends MapFunction<Tuple1<Integer>, Tuple1<Integer>> {

		private int counter = 0;

		@Override
		public Tuple1<Integer> map(Tuple1<Integer> value) throws Exception {
			counter++;
			if (counter > 3)
				threeInAll = false;
			return new Tuple1<Integer>(value.f0 * value.f0);
		}
	}

	public static final class MySink extends SinkFunction<Tuple1<Integer>> {

		@Override
		public void invoke(Tuple1<Integer> tuple) {
			result.add(tuple.f0);
		}
	}

	public static final class MyBroadcastSink extends SinkFunction<Tuple1<Integer>> {

		@Override
		public void invoke(Tuple1<Integer> tuple) {
			broadcastResult++;
		}
	}

	public static final class MyShufflesSink extends SinkFunction<Tuple1<Integer>> {

		@Override
		public void invoke(Tuple1<Integer> tuple) {
			shuffleResult++;
		}
	}

	public static final class MyFieldsSink extends SinkFunction<Tuple1<Integer>> {

		@Override
		public void invoke(Tuple1<Integer> tuple) {
			fieldsResult++;
		}
	}
	
	public static final class MyDiffFieldsSink extends SinkFunction<Tuple1<Integer>> {

		@Override
		public void invoke(Tuple1<Integer> tuple) {
			diffFieldsResult++;
		}
	}
	
	public static final class MyGraphSink extends SinkFunction<Tuple1<Integer>> {

		@Override
		public void invoke(Tuple1<Integer> tuple) {
			System.out.println("sink "+graphResult);
			graphResult++;
		}
	}

	private static Set<Integer> expected = new HashSet<Integer>();
	private static Set<Integer> result = new HashSet<Integer>();
	private static int broadcastResult = 0;
	private static int shuffleResult = 0;
	private static int fieldsResult = 0;
	private static int diffFieldsResult = 0;
	private static int graphResult = 0;
	private static int map = 0;
	private static final int PARALELISM = 1;
	private static final int MAXSOURCE = 10;
	private static boolean allInOne = false;
	private static boolean threeInAll = true;

	private static void fillExpectedList() {
		for (int i = 0; i < 10; i++) {
			expected.add(i * i);
		}
	}

	@Test
	public void mapTest() throws Exception {

		StreamExecutionEnvironment env = new StreamExecutionEnvironment();

		DataStream<Tuple1<Integer>> dataStream = env.addSource(new MySource(), 1)
				.map(new MyMap(), PARALELISM).addSink(new MySink());

		env.execute();

		fillExpectedList();

		assertTrue(expected.equals(result));
	}

	@Test
	public void broadcastSinkTest() throws Exception {
		StreamExecutionEnvironment env = new StreamExecutionEnvironment();
		DataStream<Tuple1<Integer>> dataStream = env
				.addSource(new MySource(), 1)
				.broadcast()
				.map(new MyMap(), 3)
				.addSink(new MyBroadcastSink());

		env.execute();
		assertEquals(30, broadcastResult);

	}

	@Test
	public void shuffleSinkTest() throws Exception {
		StreamExecutionEnvironment env = new StreamExecutionEnvironment();
		DataStream<Tuple1<Integer>> dataStream = env
				.addSource(new MySource(), 1)
				.map(new MyMap(), 3)
				.addSink(new MyShufflesSink());
		env.execute();
		assertEquals(10, shuffleResult);

	}

//	@Test
//	public void fieldsSinkTest() throws Exception {
//		StreamExecutionEnvironment env = new StreamExecutionEnvironment();
//		DataStream<Tuple1<Integer>> dataStream = env
//				.addSource(new MySource(), 1)
//				.partitionBy(0)
//				.map(new MyMap(), 3)
//				.addSink(new MyFieldsSink());
//
//		env.execute();
//		assertEquals(10, fieldsResult);
//
//	}

	@Test
	public void fieldsMapTest() throws Exception {
		StreamExecutionEnvironment env = new StreamExecutionEnvironment();
		DataStream<Tuple1<Integer>> dataStream = env
				.addSource(new MyFieldsSource(), 1)
				.partitionBy(0)
				.map(new MyFieldsMap(), 3)
				.addSink(new MyFieldsSink());

		env.execute();
		assertTrue(allInOne);

	}
	
	@Test
	public void diffFieldsMapTest() throws Exception {
		StreamExecutionEnvironment env = new StreamExecutionEnvironment();
		DataStream<Tuple1<Integer>> dataStream = env
				.addSource(new MyDiffFieldsSource(), 1)
				.partitionBy(0)
				.map(new MyDiffFieldsMap(), 3)
				.addSink(new MyDiffFieldsSink());

		env.execute();
		assertTrue(threeInAll);
		assertEquals(9, diffFieldsResult);

	}

//	@Test
//	public void graphTest() throws Exception {
//		for(int i=0; i<1000; i++){
//			System.out.println(i);
//			StreamExecutionEnvironment env = new StreamExecutionEnvironment();
//			DataStream<Tuple1<Integer>> dataStream = env
//					.addSource(new MySource(), 2)
//					.partitionBy(0)
//					.map(new MyMap(), 3)
//					.broadcast()
//					.addSink(new MyGraphSink(),2);
//	
//			env.execute();
//			assertEquals(40, graphResult);
//			graphResult=0;
//			map=0;
//		}
//		
//	}

}
