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
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.util.Collector;

public class MapTest {

	public static final class MySource extends SourceFunction<Tuple1<Integer>> {

		@Override
		public void invoke(Collector<Tuple1<Integer>> collector) throws Exception {
			for (int i = 0; i < 10; i++) {
				collector.collect(new Tuple1<Integer>(i));
			}
		}
	}
	
	public static final class MySource1 extends SourceFunction<Tuple1<Integer>> {

		@Override
		public void invoke(Collector<Tuple1<Integer>> collector) throws Exception {
			for (int i = 0; i < 5; i++) {
				collector.collect(new Tuple1<Integer>(i));
			}
		}
	}
	
	public static final class MySource2 extends SourceFunction<Tuple1<Integer>> {

		@Override
		public void invoke(Collector<Tuple1<Integer>> collector) throws Exception {
			for (int i = 5; i < 10; i++) {
				collector.collect(new Tuple1<Integer>(i));
			}
		}
	}
	
	public static final class MySource3 extends SourceFunction<Tuple1<Integer>> {

		@Override
		public void invoke(Collector<Tuple1<Integer>> collector) throws Exception {
			for (int i = 10; i < 15; i++) {
				collector.collect(new Tuple1<Integer>(i));
			}
		}
	}

	public static final class MyMap extends MapFunction<Tuple1<Integer>, Tuple1<Integer>> {

		@Override
		public Tuple1<Integer> map(Tuple1<Integer> value) throws Exception {
			map++;
			return new Tuple1<Integer>(value.f0 * value.f0);
		}
	}
	
	public static final class MyJoinMap extends MapFunction<Tuple1<Integer>, Tuple1<Integer>> {

		@Override
		public Tuple1<Integer> map(Tuple1<Integer> value) throws Exception {
			joinSetResult.add(value.f0);
			return new Tuple1<Integer>(value.f0);
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
			graphResult++;
		}
	}
	
	public static final class JoinSink extends SinkFunction<Tuple1<Integer>> {

		@Override
		public void invoke(Tuple1<Integer> tuple) {
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
	private static Set<Integer> fromCollectionSet = new HashSet<Integer>();
	private static List<Integer> fromCollectionFields = new ArrayList<Integer>();
	private static Set<Integer> fromCollectionDiffFieldsSet = new HashSet<Integer>();
	private static Set<Integer> singleJoinSetExpected = new HashSet<Integer>();
	private static Set<Integer> multipleJoinSetExpected = new HashSet<Integer>();
	private static Set<Integer> joinSetResult = new HashSet<Integer>();

	private static void fillExpectedList() {
		for (int i = 0; i < 10; i++) {
			expected.add(i * i);
		}
	}
	
	private static void fillFromCollectionSet() {
		if(fromCollectionSet.isEmpty()){
			for (int i = 0; i < 10; i++) {
				fromCollectionSet.add(i);
			}
		}
	}
	
	private static void fillFromCollectionFieldsSet() {
		if(fromCollectionFields.isEmpty()){
			for (int i = 0; i < MAXSOURCE; i++) {
				
				fromCollectionFields.add(5);
			}
		}
	}
	
	private static void fillFromCollectionDiffFieldsSet() {
		if(fromCollectionDiffFieldsSet.isEmpty()){
			for (int i = 0; i < 9; i++) {
				fromCollectionDiffFieldsSet.add(i);
			}
		}
	}
	
	private static void fillSingleJoinSet() {
		for (int i = 0; i < 10; i++) {
			singleJoinSetExpected.add(i);
		}
	}
	
	private static void fillMultipleJoinSet() {
		for (int i = 0; i < 15; i++) {
			multipleJoinSetExpected.add(i);
		}
	}


	@Test
	public void mapTest() throws Exception {

		StreamExecutionEnvironment env = new StreamExecutionEnvironment();

		fillFromCollectionSet();
		
		DataStream<Tuple1<Integer>> dataStream = env.fromCollection(fromCollectionSet)
				.map(new MyMap(), PARALELISM).addSink(new MySink());

		env.execute();

		fillExpectedList();

		assertTrue(expected.equals(result));
	}

	@Test
	public void broadcastSinkTest() throws Exception {
		StreamExecutionEnvironment env = new StreamExecutionEnvironment();
		
		fillFromCollectionSet();
		
		DataStream<Tuple1<Integer>> dataStream = env
				.fromCollection(fromCollectionSet)
				.broadcast()
				.map(new MyMap(), 3)
				.addSink(new MyBroadcastSink());

		env.execute();
		assertEquals(30, broadcastResult);

	}

	@Test
	public void shuffleSinkTest() throws Exception {
		StreamExecutionEnvironment env = new StreamExecutionEnvironment();
		
		fillFromCollectionSet();
		
		DataStream<Tuple1<Integer>> dataStream = env
				.fromCollection(fromCollectionSet)
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
		
		fillFromCollectionFieldsSet();
		
		DataStream<Tuple1<Integer>> dataStream = env
				.fromCollection(fromCollectionFields)
				.partitionBy(0)
				.map(new MyFieldsMap(), 3)
				.addSink(new MyFieldsSink());

		env.execute();
		assertTrue(allInOne);

	}
	
	@Test
	public void diffFieldsMapTest() throws Exception {
		StreamExecutionEnvironment env = new StreamExecutionEnvironment();
		
		fillFromCollectionDiffFieldsSet();
		
		DataStream<Tuple1<Integer>> dataStream = env
				.fromCollection(fromCollectionDiffFieldsSet)
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
	
	@Test
	public void singleConnectWithTest() throws Exception {
		
		StreamExecutionEnvironment env = new StreamExecutionEnvironment();

		DataStream<Tuple1<Integer>> source1 = env.addSource(new MySource1(),
				1);
		
		DataStream<Tuple1<Integer>> source2 = env
				.addSource(new MySource2(), 1)
				.connectWith(source1)
				.partitionBy(0)
				.map(new MyJoinMap(), 1)
				.addSink(new JoinSink());

		env.execute();
		
		fillSingleJoinSet();
		
		assertEquals(singleJoinSetExpected, joinSetResult);
	}
	
	@Test
	public void multipleConnectWithTest() throws Exception {
		
		StreamExecutionEnvironment env = new StreamExecutionEnvironment();

		DataStream<Tuple1<Integer>> source1 = env.addSource(new MySource1(),
				1);
		
		DataStream<Tuple1<Integer>> source2 = env.addSource(new MySource2(),
				1);
		DataStream<Tuple1<Integer>> source3 = env
				.addSource(new MySource3(), 1)
				.connectWith(source1, source2)
				.partitionBy(0)
				.map(new MyJoinMap(), 1)
				.addSink(new JoinSink());

		env.execute();
		
		fillMultipleJoinSet();
		
		assertEquals(multipleJoinSetExpected, joinSetResult);
	}

}
