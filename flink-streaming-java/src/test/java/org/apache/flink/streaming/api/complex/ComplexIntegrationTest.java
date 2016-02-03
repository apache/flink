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

package org.apache.flink.streaming.api.complex;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.TimestampExtractor;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.DeltaTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.apache.flink.util.Collector;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("serial")
public class ComplexIntegrationTest extends StreamingMultipleProgramsTestBase {

	// *************************************************************************
	// GENERAL SETUP
	// *************************************************************************

	private String resultPath1;
	private String resultPath2;
	private String expected1;
	private String expected2;
	

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Before
	public void before() throws Exception {
		resultPath1 = tempFolder.newFile().toURI().toString();
		resultPath2 = tempFolder.newFile().toURI().toString();
		expected1 = "";
		expected2 = "";
	}

	@After
	public void after() throws Exception {
		compareResultsByLinesInMemory(expected1, resultPath1);
		compareResultsByLinesInMemory(expected2, resultPath2);
	}

	// *************************************************************************
	// INTEGRATION TESTS
	// *************************************************************************

	@Test
	public void complexIntegrationTest1() throws Exception {
		//Testing data stream splitting with tuples

		expected1 = "";
		for (int i = 0; i < 8; i++) {
			expected1 += "(10,(a,1))\n";
		}
		//i == 8
		expected1 += "(10,(a,1))";

		expected2 = "";
		for (int i = 0; i < 18; i++) {
			expected2 += "(20,(a,1))\n";
		}
		//i == 18
		expected2 += "(20,(a,1))";

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Tuple2<Long, Tuple2<String, Long>>> sourceStream1 = env.addSource(new TupleSource()).setParallelism(1);

		IterativeStream<Tuple2<Long, Tuple2<String, Long>>> it = sourceStream1.map(new MapFunction<Tuple2<Long, Tuple2<String, Long>>,Tuple2<Long, Tuple2<String, Long>>>(){

					Tuple2<Long, Tuple2<String, Long>> result = new Tuple2<>(
							0L, new Tuple2<>("", 0L));

					@Override
					public Tuple2<Long, Tuple2<String, Long>> map(
							Tuple2<Long, Tuple2<String, Long>> value) throws Exception {
						result.f0 = result.f0 + value.f0;
						result.f1 = value.f1;
						return result;
			}
			
		})
				.setParallelism(1).filter(new FilterFunction
				<Tuple2<Long, Tuple2<String, Long>>>() {

			@Override
			public boolean filter(Tuple2<Long, Tuple2<String, Long>> value) throws Exception {
				return value.f0 < 20;
			}
		}).iterate(5000);

		SplitStream<Tuple2<Long, Tuple2<String, Long>>> step = it.map(new IncrementMap()).split(new
				MyOutputSelector());
		it.closeWith(step.select("iterate"));

		step.select("firstOutput")
				.writeAsText(resultPath1, FileSystem.WriteMode.OVERWRITE);
		step.select("secondOutput")
				.writeAsText(resultPath2, FileSystem.WriteMode.OVERWRITE);

		env.execute();
	}

	// Disabled, because it depends on strange behaviour, for example of the sum() function.
	// This test evens fails, for example, if the order of only two lines in the "input" is changed.
	@SuppressWarnings("unchecked")
	@Ignore
	@Test
	public void complexIntegrationTest2() throws Exception {
		//Testing POJO source, grouping by multiple filds and windowing with timestamp

		expected1 = "water_melon-b\n" + "water_melon-b\n" + "water_melon-b\n" + "water_melon-b\n" +
				"water_melon-b\n" + "water_melon-b\n" + "water_melon-b\n" + "water_melon-b\n" + "water_melon-b\n" +
				"water_melon-b\n" + "water_melon-b\n" + "water_melon-b\n" + "water_melon-b\n" + "water_melon-b\n" +
				"water_melon-b\n" + "water_melon-b\n" + "water_melon-b\n" + "water_melon-b\n" + "water_melon-b\n" +
				"water_melon-b\n" + "orange-b\n" + "orange-b\n" + "orange-b\n" + "orange-b\n" + "orange-b\n" +
				"orange-b\n" + "orange-c\n" + "orange-c\n" + "orange-c\n" + "orange-c\n" + "orange-d\n" + "orange-d\n" +
				"peach-d\n" + "peach-d\n";

		List<Tuple5<Integer, String, Character, Double, Boolean>> input = Arrays.asList(
				new Tuple5<>(1, "apple", 'j', 0.1, false),
				new Tuple5<>(1, "peach", 'b', 0.8, false),
				new Tuple5<>(1, "orange", 'c', 0.7, true),
				new Tuple5<>(2, "apple", 'd', 0.5, false),
				new Tuple5<>(2, "peach", 'j', 0.6, false),
				new Tuple5<>(3, "orange", 'b', 0.2, true),
				new Tuple5<>(6, "apple", 'c', 0.1, false),
				new Tuple5<>(7, "peach", 'd', 0.4, false),
				new Tuple5<>(8, "orange", 'j', 0.2, true),
				new Tuple5<>(10, "apple", 'b', 0.1, false),
				new Tuple5<>(10, "peach", 'c', 0.5, false),
				new Tuple5<>(11, "orange", 'd', 0.3, true),
				new Tuple5<>(11, "apple", 'j', 0.3, false),
				new Tuple5<>(12, "peach", 'b', 0.9, false),
				new Tuple5<>(13, "orange", 'c', 0.7, true),
				new Tuple5<>(15, "apple", 'd', 0.2, false),
				new Tuple5<>(16, "peach", 'j', 0.8, false),
				new Tuple5<>(16, "orange", 'b', 0.8, true),
				new Tuple5<>(16, "apple", 'c', 0.1, false),
				new Tuple5<>(17, "peach", 'd', 1.0, true));

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().enableTimestamps();

		SingleOutputStreamOperator<Tuple5<Integer, String, Character, Double, Boolean>, DataStreamSource<Tuple5<Integer, String, Character, Double, Boolean>>> sourceStream21 = env.fromCollection(input);
		DataStream<OuterPojo> sourceStream22 = env.addSource(new PojoSource());

		sourceStream21
				.assignTimestamps(new MyTimestampExtractor())
				.keyBy(2, 2)
				.timeWindow(Time.of(10, TimeUnit.MILLISECONDS), Time.of(4, TimeUnit.MILLISECONDS))
				.maxBy(3)
				.map(new MyMapFunction2())
				.flatMap(new MyFlatMapFunction())
				.connect(sourceStream22)
				.map(new MyCoMapFunction())
				.writeAsText(resultPath1, FileSystem.WriteMode.OVERWRITE);

		env.execute();
	}

	// Ignore because the count(10_000) window actually only emits one element during processing
	// and all the rest in close()
	@SuppressWarnings("unchecked")
	@Ignore
	@Test
	public void complexIntegrationTest3() throws Exception {
		//Heavy prime factorisation with maps and flatmaps

		expected1 = "541\n" + "1223\n" + "3319\n" + "5851\n" + "1987\n" + "8387\n" + "15907\n" + "10939\n" +
				"4127\n" + "2477\n" + "6737\n" + "13421\n" + "4987\n" + "4999\n" + "18451\n" + "9283\n" + "7499\n" +
				"16937\n" + "11927\n" + "9973\n" + "14431\n" + "19507\n" + "12497\n" + "17497\n" + "14983\n" +
				"19997\n";

		expected1 = "541\n" + "1223\n" + "1987\n" + "2741\n" + "3571\n" + "10939\n" + "4409\n" +
				"5279\n" + "11927\n" + "6133\n" + "6997\n" + "12823\n" + "7919\n" + "8831\n" +
				"13763\n" + "9733\n" + "9973\n" + "14759\n" + "15671\n" + "16673\n" + "17659\n" +
				"18617\n";

		for (int i = 2; i < 100; i++) {
			expected2 += "(" + i + "," + 20000 / i + ")\n";
		}
		for (int i = 19901; i < 20000; i++) {
			expected2 += "(" + i + "," + 20000 / i + ")\n";
		}
		//i == 20000
		expected2 += "(" + 20000 + "," + 1 + ")";

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// set to parallelism 1 because otherwise we don't know which elements go to which parallel
		// count-window.
		env.setParallelism(1);

		env.setBufferTimeout(0);

		DataStream<Long> sourceStream31 = env.generateSequence(1, 10000);
		DataStream<Long> sourceStream32 = env.generateSequence(10001, 20000);


		sourceStream31.filter(new PrimeFilterFunction())
				.windowAll(GlobalWindows.create())
				.trigger(PurgingTrigger.of(CountTrigger.of(100)))
				.max(0)
				.union(sourceStream32.filter(new PrimeFilterFunction())
						.windowAll(GlobalWindows.create())
						.trigger(PurgingTrigger.of(CountTrigger.of(100)))
						.max(0))
				.writeAsText(resultPath1, FileSystem.WriteMode.OVERWRITE);

		sourceStream31
			.flatMap(new DivisorsFlatMapFunction())
			.union(sourceStream32.flatMap(new DivisorsFlatMapFunction()))
			.map(new MapFunction<Long, Tuple2<Long,Integer>>() {

			@Override
			public Tuple2<Long, Integer> map(Long value) throws Exception {
				return new Tuple2<>(value, 1);
			}
		})
				.keyBy(0)
				.window(GlobalWindows.create())
				.trigger(PurgingTrigger.of(CountTrigger.of(10_000)))
				.sum(1)

//				.filter(new FilterFunction<Tuple2<Long, Integer>>() {
//
//					@Override
//					public boolean filter(Tuple2<Long, Integer> value) throws Exception {
//						return value.f0 < 100 || value.f0 > 19900;
//					}
//				})
			.print();
//				.writeAsText(resultPath2, FileSystem.WriteMode.OVERWRITE);

		env.execute();
	}

	@Test
	@Ignore
	@SuppressWarnings("unchecked, rawtypes")
	public void complexIntegrationTest4() throws Exception {
		//Testing mapping and delta-policy windowing with custom class

		expected1 = "((100,100),0)\n" + "((120,122),5)\n" + "((121,125),6)\n" + "((138,144),9)\n" +
			"((139,147),10)\n" + "((156,166),13)\n" + "((157,169),14)\n" + "((174,188),17)\n" + "((175,191),18)\n" +
			"((192,210),21)\n" + "((193,213),22)\n" + "((210,232),25)\n" + "((211,235),26)\n" + "((228,254),29)\n" +
			"((229,257),30)\n" + "((246,276),33)\n" + "((247,279),34)\n" + "((264,298),37)\n" + "((265,301),38)\n" +
			"((282,320),41)\n" + "((283,323),42)\n" + "((300,342),45)\n" + "((301,345),46)\n" + "((318,364),49)\n" +
			"((319,367),50)\n" + "((336,386),53)\n" + "((337,389),54)\n" + "((354,408),57)\n" + "((355,411),58)\n" +
			"((372,430),61)\n" + "((373,433),62)\n" + "((390,452),65)\n" + "((391,455),66)\n" + "((408,474),69)\n" +
			"((409,477),70)\n" + "((426,496),73)\n" + "((427,499),74)\n" + "((444,518),77)\n" + "((445,521),78)\n" +
			"((462,540),81)\n" + "((463,543),82)\n" + "((480,562),85)\n" + "((481,565),86)\n" + "((498,584),89)\n" +
			"((499,587),90)\n" + "((516,606),93)\n" + "((517,609),94)\n" + "((534,628),97)\n" + "((535,631),98)";

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		TupleSerializer<Tuple2<Rectangle, Integer>> deltaSerializer = new TupleSerializer<>((Class) Tuple2.class,
			new TypeSerializer[] {new KryoSerializer<>(Rectangle.class, env.getConfig()),
			IntSerializer.INSTANCE});

		env.addSource(new RectangleSource())
				.global()
				.map(new RectangleMapFunction())
				.windowAll(GlobalWindows.create())
				.trigger(PurgingTrigger.of(DeltaTrigger.of(0.0, new MyDelta(), deltaSerializer)))
				.apply(new MyWindowMapFunction())
				.writeAsText(resultPath1, FileSystem.WriteMode.OVERWRITE);

		env.execute();
	}

	private static class MyDelta implements DeltaFunction<Tuple2<Rectangle, Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public double getDelta(Tuple2<Rectangle, Integer> oldDataPoint, Tuple2<Rectangle,
				Integer> newDataPoint) {
			return (newDataPoint.f0.b - newDataPoint.f0.a) - (oldDataPoint.f0.b - oldDataPoint.f0.a);
		}
	}


	@Test
	public void complexIntegrationTest5() throws Exception {
		//Turning on and off chaining

		expected1 = "1\n" + "2\n" + "2\n" + "3\n" + "3\n" + "3\n" + "4\n" + "4\n" + "4\n" + "4\n" + "5\n" + "5\n" +
				"5\n" + "5\n" + "5\n";

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Set to parallelism 1 to make it deterministic, otherwise, it is not clear which
		// elements will go to which parallel instance of the fold
		env.setParallelism(1);
		
		env.setBufferTimeout(0);

		DataStream<Long> dataStream51 = env.generateSequence(1, 5)
				.map(new MapFunction<Long, Long>() {

					@Override
					public Long map(Long value) throws Exception {
						return value;
					}
				}).startNewChain()
				.filter(new FilterFunction<Long>() {

					@Override
					public boolean filter(Long value) throws Exception {
						return true;
					}
				}).disableChaining()
				.flatMap(new SquareFlatMapFunction());

		DataStream<Long> dataStream53 = dataStream51.map(new MapFunction<Long, Long>() {

			@Override
			public Long map(Long value) throws Exception {
				return value;
			}
		});


		dataStream53.writeAsText(resultPath1, FileSystem.WriteMode.OVERWRITE);

		env.execute();
	}


	@Test
	@Ignore
	public void complexIntegrationTest6() throws Exception {
		//Testing java collections and date-time types

		expected1 = "(6,(a,6))\n" + "(6,(b,3))\n" + "(6,(c,4))\n" + "(6,(d,2))\n" + "(6,(f,2))\n" +
				"(7,(a,1))\n" + "(7,(b,2))\n" + "(7,(c,3))\n" + "(7,(d,1))\n" + "(7,(e,1))\n" + "(7,(f,1))\n" +
				"(8,(a,6))\n" + "(8,(b,4))\n" + "(8,(c,5))\n" + "(8,(d,1))\n" + "(8,(e,2))\n" + "(8,(f,2))\n" +
				"(9,(a,4))\n" + "(9,(b,4))\n" + "(9,(c,7))\n" + "(9,(d,3))\n" + "(9,(e,1))\n" + "(9,(f,2))\n" +
				"(10,(a,3))\n" + "(10,(b,2))\n" + "(10,(c,3))\n" + "(10,(d,2))\n" + "(10,(e,1))\n" + "(10,(f,1))";
		expected2 = "[a, a, c, c, d, f]\n" + "[a, b, b, d]\n" + "[a, a, a, b, c, c, f]\n" + "[a, d, e]\n" +
				"[b, b, c, c, c, f]\n" + "[a, a, a, a, b, b, c, c, e]\n" + "[a, a, b, b, c, c, c, d, e, f, f]\n" +
				"[a, a, a, b, c, c, c, d, d, f]\n" + "[a, b, b, b, c, c, c, c, d, e, f]\n" +
				"[a, a, a, b, b, c, c, c, d, d, e, f]";

		SimpleDateFormat ft = new SimpleDateFormat("dd-MM-yyyy");

		ArrayList<Tuple2<Date, HashMap<Character, Integer>>> sales = new ArrayList<>();
		HashMap<Character, Integer> sale1 = new HashMap<>();
		sale1.put('a', 2);
		sale1.put('c', 2);
		sale1.put('d', 1);
		sale1.put('f', 1);
		sales.add(new Tuple2<>(ft.parse("03-06-2014"), sale1));

		HashMap<Character, Integer> sale2 = new HashMap<>();
		sale2.put('a', 1);
		sale2.put('b', 2);
		sale2.put('d', 1);
		sales.add(new Tuple2<>(ft.parse("10-06-2014"), sale2));

		HashMap<Character, Integer> sale3 = new HashMap<>();
		sale3.put('a', 3);
		sale3.put('b', 1);
		sale3.put('c', 2);
		sale3.put('f', 1);
		sales.add(new Tuple2<>(ft.parse("29-06-2014"), sale3));

		HashMap<Character, Integer> sale4 = new HashMap<>();
		sale4.put('a', 1);
		sale4.put('d', 1);
		sale4.put('e', 1);
		sales.add(new Tuple2<>(ft.parse("15-07-2014"), sale4));

		HashMap<Character, Integer> sale5 = new HashMap<>();
		sale5.put('b', 2);
		sale5.put('c', 3);
		sale5.put('f', 1);
		sales.add(new Tuple2<>(ft.parse("24-07-2014"), sale5));

		HashMap<Character, Integer> sale6 = new HashMap<>();
		sale6.put('a', 4);
		sale6.put('b', 2);
		sale6.put('c', 2);
		sale6.put('e', 1);
		sales.add(new Tuple2<>(ft.parse("17-08-2014"), sale6));

		HashMap<Character, Integer> sale7 = new HashMap<>();
		sale7.put('a', 2);
		sale7.put('b', 2);
		sale7.put('c', 3);
		sale7.put('d', 1);
		sale7.put('e', 1);
		sale7.put('f', 2);
		sales.add(new Tuple2<>(ft.parse("27-08-2014"), sale7));

		HashMap<Character, Integer> sale8 = new HashMap<>();
		sale8.put('a', 3);
		sale8.put('b', 1);
		sale8.put('c', 3);
		sale8.put('d', 2);
		sale8.put('f', 1);
		sales.add(new Tuple2<>(ft.parse("16-09-2014"), sale8));

		HashMap<Character, Integer> sale9 = new HashMap<>();
		sale9.put('a', 1);
		sale9.put('b', 3);
		sale9.put('c', 4);
		sale9.put('d', 1);
		sale9.put('e', 1);
		sale9.put('f', 1);
		sales.add(new Tuple2<>(ft.parse("25-09-2014"), sale9));

		HashMap<Character, Integer> sale10 = new HashMap<>();
		sale10.put('a', 3);
		sale10.put('b', 2);
		sale10.put('c', 3);
		sale10.put('d', 2);
		sale10.put('e', 1);
		sale10.put('f', 1);
		sales.add(new Tuple2<>(ft.parse("01-10-2014"), sale10));

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().enableTimestamps();

		DataStream<Tuple2<Date, HashMap<Character, Integer>>> sourceStream6 = env.fromCollection(sales);
		sourceStream6
				.assignTimestamps(new Timestamp6())
				.timeWindowAll(Time.of(1, TimeUnit.MILLISECONDS))
				.reduce(new SalesReduceFunction())
				.flatMap(new FlatMapFunction6())
				.writeAsText(resultPath1, FileSystem.WriteMode.OVERWRITE);

		sourceStream6.map(new MapFunction6())
				.writeAsText(resultPath2, FileSystem.WriteMode.OVERWRITE);

		env.execute();

	}

	// *************************************************************************
	// FUNCTIONS
	// *************************************************************************

	private static class MyMapFunction2 implements MapFunction<Tuple5<Integer, String, Character, Double, Boolean>, Tuple4<Integer, String,
			Double, Boolean>> {

		@Override
		public Tuple4<Integer, String, Double, Boolean> map(Tuple5<Integer, String, Character, Double,
				Boolean> value) throws Exception {
			return new Tuple4<>(value.f0, value.f1 + "-" + value.f2,
					value.f3, value.f4);
		}

	}

	private static class PojoSource implements SourceFunction<OuterPojo> {
		private static final long serialVersionUID = 1L;

		long cnt = 0;

		@Override
		public void run(SourceContext<OuterPojo> ctx) throws Exception {
			for (int i = 0; i < 20; i++) {
				OuterPojo result = new OuterPojo(new InnerPojo(cnt / 2, "water_melon-b"), 2L);
				ctx.collect(result);
			}
		}

		@Override
		public void cancel() {

		}
	}

	private static class TupleSource implements SourceFunction<Tuple2<Long, Tuple2<String, Long>>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void run(SourceContext<Tuple2<Long, Tuple2<String, Long>>> ctx) throws Exception {
			for (int i = 0; i < 20; i++) {
				Tuple2<Long, Tuple2<String, Long>> result = new Tuple2<>(1L, new Tuple2<>("a", 1L));
				ctx.collect(result);
			}
		}

		@Override
		public void cancel() {

		}
	}

	private class IncrementMap implements MapFunction<Tuple2<Long, Tuple2<String, Long>>, Tuple2<Long, Tuple2<String,
			Long>>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<Long, Tuple2<String, Long>> map(Tuple2<Long, Tuple2<String, Long>> value) throws Exception {
			return new Tuple2<>(value.f0 + 1, value.f1);
		}
	}

	private static class MyTimestampExtractor implements TimestampExtractor<Tuple5<Integer, String, Character, Double, Boolean>> {
		private static final long serialVersionUID = 1L;

		@Override
		public long extractTimestamp(Tuple5<Integer, String, Character, Double, Boolean> value, long currentTimestamp) {
			return (long) value.f0;
		}

		@Override
		public long extractWatermark(Tuple5<Integer, String, Character, Double, Boolean> value,
				long currentTimestamp) {
			return (long) value.f0 - 1;
		}

		@Override
		public long getCurrentWatermark() {
			return Long.MIN_VALUE;
		}
	}

	private static class MyFlatMapFunction implements FlatMapFunction<Tuple4<Integer, String, Double,
			Boolean>, OuterPojo> {
		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(Tuple4<Integer, String, Double, Boolean> value, Collector<OuterPojo> out) throws
				Exception {
			if (value.f3) {
				for (int i = 0; i < 2; i++) {
					out.collect(new OuterPojo(new InnerPojo((long) value.f0, value.f1), (long) i));
				}
			}
		}
	}

	private class MyCoMapFunction implements CoMapFunction<OuterPojo, OuterPojo, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public String map1(OuterPojo value) {
			return value.f0.f1;
		}

		@Override
		public String map2(OuterPojo value) {
			return value.f0.f1;
		}
	}

	private class MyOutputSelector implements OutputSelector<Tuple2<Long, Tuple2<String, Long>>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Iterable<String> select(Tuple2<Long, Tuple2<String, Long>> value) {
			List<String> output = new ArrayList<>();
			if (value.f0 == 10) {
				output.add("iterate");
				output.add("firstOutput");
			} else if (value.f0 == 20) {
				output.add("secondOutput");
			} else {
				output.add("iterate");
			}
			return output;
		}
	}

	private static class PrimeFilterFunction implements FilterFunction<Long> {
		private static final long serialVersionUID = 1L;

		@Override
		public boolean filter(Long value) throws Exception {
			if (value < 2) {
				return false;
			} else {
				for (long i = 2; i < value; i++) {
					if (value % i == 0) {
						return false;
					}
				}
			}
			return true;
		}
	}

	private static class DivisorsFlatMapFunction implements FlatMapFunction<Long, Long> {
		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(Long value, Collector<Long> out) throws Exception {
			for (long i = 2; i <= value; i++) {
				if (value % i == 0) {
					out.collect(i);
				}
			}
		}
	}

	private static class RectangleSource extends RichSourceFunction<Rectangle> {
		private static final long serialVersionUID = 1L;
		private transient Rectangle rectangle;

		public void open(Configuration parameters) throws Exception {
			rectangle = new Rectangle(100, 100);
		}

		@Override
		public void run(SourceContext<Rectangle> ctx) throws Exception {
			// emit once as the initializer of the delta trigger
			ctx.collect(rectangle);
			for (int i = 0; i < 100; i++) {
				ctx.collect(rectangle);
				rectangle = rectangle.next();
			}
		}

		@Override
		public void cancel() {
		}
	}

	private static class RectangleMapFunction implements MapFunction<Rectangle, Tuple2<Rectangle, Integer>> {
		private static final long serialVersionUID = 1L;
		private int counter = 0;

		@Override
		public Tuple2<Rectangle, Integer> map(Rectangle value) throws Exception {
			return new Tuple2<>(value, counter++);
		}
	}

	private static class MyWindowMapFunction implements AllWindowFunction<Iterable<Tuple2<Rectangle, Integer>>, Tuple2<Rectangle, Integer>, GlobalWindow> {
		private static final long serialVersionUID = 1L;

		@Override
		public void apply(GlobalWindow window, Iterable<Tuple2<Rectangle, Integer>> values, Collector<Tuple2<Rectangle,
				Integer>> out) throws Exception {
			out.collect(values.iterator().next());
		}
	}

	private static class SquareFlatMapFunction implements FlatMapFunction<Long, Long> {
		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(Long value, Collector<Long> out) throws Exception {
			for (long i = 0; i < value; i++) {
				out.collect(value);
			}
		}
	}

	private static class Timestamp6 implements TimestampExtractor<Tuple2<Date, HashMap<Character, Integer>>> {

		@Override
		public long extractTimestamp(Tuple2<Date, HashMap<Character, Integer>> value,
				long currentTimestamp) {
			Calendar cal = Calendar.getInstance();
			cal.setTime(value.f0);
			return 12 * (cal.get(Calendar.YEAR)) + cal.get(Calendar.MONTH);
		}

		@Override
		public long extractWatermark(Tuple2<Date, HashMap<Character, Integer>> value,
				long currentTimestamp) {
			Calendar cal = Calendar.getInstance();
			cal.setTime(value.f0);
			return 12 * (cal.get(Calendar.YEAR)) + cal.get(Calendar.MONTH) - 1;
		}

		@Override
		public long getCurrentWatermark() {
			return 0;
		}
	}

	private static class SalesReduceFunction implements ReduceFunction<Tuple2<Date, HashMap<Character, Integer>>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<Date, HashMap<Character, Integer>> reduce(Tuple2<Date, HashMap<Character, Integer>> value1,
				Tuple2<Date,
						HashMap<Character, Integer>> value2) throws Exception {
			HashMap<Character, Integer> map1 = value1.f1;
			HashMap<Character, Integer> map2 = value2.f1;
			for (Character key : map2.keySet()) {
				Integer volume1 = map1.get(key);
				Integer volume2 = map2.get(key);
				if (volume1 == null) {
					volume1 = 0;
				}
				map1.put(key, volume1 + volume2);
			}
			return new Tuple2<>(value2.f0, map1);
		}
	}

	private static class FlatMapFunction6 implements FlatMapFunction<Tuple2<Date, HashMap<Character, Integer>>, Tuple2<Integer,
			Tuple2<Character, Integer>>> {

		@Override
		public void flatMap(Tuple2<Date, HashMap<Character, Integer>> value, Collector<Tuple2<Integer,
				Tuple2<Character, Integer>>> out) throws Exception {
			Calendar cal = Calendar.getInstance();
			cal.setTime(value.f0);
			for (Character key : value.f1.keySet()) {
				out.collect(new Tuple2<>(cal.get(Calendar.MONTH)
						+ 1,
						new Tuple2<>(key, value.f1.get(key))));
			}
		}
	}

	private static class MapFunction6 implements MapFunction<Tuple2<Date, HashMap<Character, Integer>>, ArrayList<Character>> {

		@Override
		public ArrayList<Character> map(Tuple2<Date, HashMap<Character, Integer>> value)
				throws Exception {
			ArrayList<Character> list = new ArrayList<>();
			for (Character ch : value.f1.keySet()) {
				for (int i = 0; i < value.f1.get(ch); i++) {
					list.add(ch);
				}
			}
			Collections.sort(list);
			return list;
		}
	}

	// *************************************************************************
	// DATA TYPES
	// *************************************************************************

	//Flink Pojo
	public static class InnerPojo {
		public Long f0;
		public String f1;

		//default constructor to qualify as Flink POJO
		InnerPojo(){}

		public InnerPojo(Long f0, String f1) {
			this.f0 = f0;
			this.f1 = f1;
		}

		@Override
		public String toString() {
			return "POJO(" + f0 + "," + f1 + ")";
		}
	}

	// Nested class serialized with Kryo
	public static class OuterPojo {
		public InnerPojo f0;
		public Long f1;

		public OuterPojo(InnerPojo f0, Long f1) {
			this.f0 = f0;
			this.f1 = f1;
		}

		@Override
		public String toString() {
			return "POJO(" + f0 + "," + f1 + ")";
		}
	}

	public static class Rectangle {

		public int a;
		public int b;

		//default constructor to qualify as Flink POJO
		public Rectangle() {}

		public Rectangle(int a, int b) {
			this.a = a;
			this.b = b;
		}

		public Rectangle next() {
			return new Rectangle(a + (b % 11), b + (a % 9));
		}

		@Override
		public String toString() {
			return "(" + a + "," + b + ")";
		}

	}

}
