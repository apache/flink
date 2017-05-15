/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.streaming.runtime;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.StoppableFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.testutils.MultiShotLatch;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for timestamps, watermarks, and event-time sources.
 */
@SuppressWarnings("serial")
public class TimestampITCase extends TestLogger {

	private static final int NUM_TASK_MANAGERS = 2;
	private static final int NUM_TASK_SLOTS = 3;
	private static final int PARALLELISM = NUM_TASK_MANAGERS * NUM_TASK_SLOTS;

	// this is used in some tests to synchronize
	static MultiShotLatch latch;


	private static LocalFlinkMiniCluster cluster;

	@Before
	public void setupLatch() {
		// ensure that we get a fresh latch for each test
		latch = new MultiShotLatch();
	}


	@BeforeClass
	public static void startCluster() {
		Configuration config = new Configuration();
		config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, NUM_TASK_MANAGERS);
		config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, NUM_TASK_SLOTS);
		config.setLong(TaskManagerOptions.MANAGED_MEMORY_SIZE, 12L);

		cluster = new LocalFlinkMiniCluster(config, false);

		cluster.start();

		TestStreamEnvironment.setAsContext(cluster, PARALLELISM);
	}

	@AfterClass
	public static void shutdownCluster() {
		cluster.shutdown();
		cluster = null;

		TestStreamEnvironment.unsetAsContext();
	}

	/**
	 * These check whether custom timestamp emission works at sources and also whether timestamps
	 * arrive at operators throughout a topology.
	 *
	 * <p>
	 * This also checks whether watermarks keep propagating if a source closes early.
	 *
	 * <p>
	 * This only uses map to test the workings of watermarks in a complete, running topology. All
	 * tasks and stream operators have dedicated tests that test the watermark propagation
	 * behaviour.
	 */
	@Test
	public void testWatermarkPropagation() throws Exception {
		final int NUM_WATERMARKS = 10;

		long initialTime = 0L;

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(PARALLELISM);
		env.getConfig().disableSysoutLogging();

		DataStream<Integer> source1 = env.addSource(new MyTimestampSource(initialTime, NUM_WATERMARKS));
		DataStream<Integer> source2 = env.addSource(new MyTimestampSource(initialTime, NUM_WATERMARKS / 2));

		source1.union(source2)
				.map(new IdentityMap())
				.connect(source2).map(new IdentityCoMap())
				.transform("Custom Operator", BasicTypeInfo.INT_TYPE_INFO, new CustomOperator(true))
				.addSink(new DiscardingSink<Integer>());

		env.execute();

		// verify that all the watermarks arrived at the final custom operator
		for (int i = 0; i < PARALLELISM; i++) {
			// we are only guaranteed to see NUM_WATERMARKS / 2 watermarks because the
			// other source stops emitting after that
			for (int j = 0; j < NUM_WATERMARKS / 2; j++) {
				if (!CustomOperator.finalWatermarks[i].get(j).equals(new Watermark(initialTime + j))) {
					System.err.println("All Watermarks: ");
					for (int k = 0; k <= NUM_WATERMARKS / 2; k++) {
						System.err.println(CustomOperator.finalWatermarks[i].get(k));
					}

					fail("Wrong watermark.");
				}
			}
			
			assertEquals(Watermark.MAX_WATERMARK,
					CustomOperator.finalWatermarks[i].get(CustomOperator.finalWatermarks[i].size()-1));
		}
	}

	@Test
	public void testWatermarkPropagationNoFinalWatermarkOnStop() throws Exception {
		
		// for this test to work, we need to be sure that no other jobs are being executed
		while (!cluster.getCurrentlyRunningJobsJava().isEmpty()) {
			Thread.sleep(100);
		}
		
		final int NUM_WATERMARKS = 10;

		long initialTime = 0L;

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(PARALLELISM);
		env.getConfig().disableSysoutLogging();

		DataStream<Integer> source1 = env.addSource(new MyTimestampSourceInfinite(initialTime, NUM_WATERMARKS));
		DataStream<Integer> source2 = env.addSource(new MyTimestampSourceInfinite(initialTime, NUM_WATERMARKS / 2));

		source1.union(source2)
				.map(new IdentityMap())
				.connect(source2).map(new IdentityCoMap())
				.transform("Custom Operator", BasicTypeInfo.INT_TYPE_INFO, new CustomOperator(true))
				.addSink(new DiscardingSink<Integer>());

		new Thread("stopper") {
			@Override
			public void run() {
				try {
					// try until we get the running jobs
					List<JobID> running;
					while ((running = cluster.getCurrentlyRunningJobsJava()).isEmpty()) {
						Thread.sleep(10);
					}

					JobID id = running.get(0);
					
					// send stop until the job is stopped
					do {
						try {
							cluster.stopJob(id);
						}
						catch (Exception e) {
							if (e.getCause() instanceof IllegalStateException) {
								// this means the job is not yet ready to be stopped,
								// for example because it is still in CREATED state
								// we ignore the exception 
							} else {
								// other problem
								throw e;
							}
						}
						Thread.sleep(10);
					}
					while (!cluster.getCurrentlyRunningJobsJava().isEmpty());
				}
				catch (Throwable t) {
					t.printStackTrace();
				}
			}
		}.start();
		
		env.execute();

		// verify that all the watermarks arrived at the final custom operator
		for (List<Watermark> subtaskWatermarks : CustomOperator.finalWatermarks) {
			
			// we are only guaranteed to see NUM_WATERMARKS / 2 watermarks because the
			// other source stops emitting after that
			for (int j = 0; j < subtaskWatermarks.size(); j++) {
				if (subtaskWatermarks.get(j).getTimestamp() != initialTime + j) {
					System.err.println("All Watermarks: ");
					for (int k = 0; k <= NUM_WATERMARKS / 2; k++) {
						System.err.println(subtaskWatermarks.get(k));
					}

					fail("Wrong watermark.");
				}
			}
			
			// if there are watermarks, the final one must not be the MAX watermark
			if (subtaskWatermarks.size() > 0) {
				assertNotEquals(Watermark.MAX_WATERMARK,
						subtaskWatermarks.get(subtaskWatermarks.size()-1));
			}
		}
	}

	/**
	 * These check whether timestamps are properly assigned at the sources and handled in
	 * network transmission and between chained operators when timestamps are enabled.
	 */
	@Test
	public void testTimestampHandling() throws Exception {
		final int NUM_ELEMENTS = 10;


		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(PARALLELISM);
		env.getConfig().disableSysoutLogging();

		DataStream<Integer> source1 = env.addSource(new MyTimestampSource(0L, NUM_ELEMENTS));
		DataStream<Integer> source2 = env.addSource(new MyTimestampSource(0L, NUM_ELEMENTS));

		source1
				.map(new IdentityMap())
				.connect(source2).map(new IdentityCoMap())
				.transform("Custom Operator", BasicTypeInfo.INT_TYPE_INFO, new TimestampCheckingOperator())
				.addSink(new DiscardingSink<Integer>());


		env.execute();
	}

	/**
	 * These check whether timestamps are properly ignored when they are disabled.
	 */
	@Test
	public void testDisabledTimestamps() throws Exception {
		final int NUM_ELEMENTS = 10;
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		env.setParallelism(PARALLELISM);
		env.getConfig().disableSysoutLogging();
		
		DataStream<Integer> source1 = env.addSource(new MyNonWatermarkingSource(NUM_ELEMENTS));
		DataStream<Integer> source2 = env.addSource(new MyNonWatermarkingSource(NUM_ELEMENTS));

		source1
				.map(new IdentityMap())
				.connect(source2).map(new IdentityCoMap())
				.transform("Custom Operator", BasicTypeInfo.INT_TYPE_INFO, new DisabledTimestampCheckingOperator())
				.addSink(new DiscardingSink<Integer>());
		
		env.execute();
	}

	/**
	 * This tests whether timestamps are properly extracted in the timestamp
	 * extractor and whether watermarks are also correctly forwared from this with the auto watermark
	 * interval.
	 */
	@Test
	public void testTimestampExtractorWithAutoInterval() throws Exception {
		final int NUM_ELEMENTS = 10;

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.getConfig().setAutoWatermarkInterval(10);
		env.setParallelism(1);
		env.getConfig().disableSysoutLogging();


		DataStream<Integer> source1 = env.addSource(new SourceFunction<Integer>() {
			@Override
			public void run(SourceContext<Integer> ctx) throws Exception {
				int index = 1;
				while (index <= NUM_ELEMENTS) {
					ctx.collect(index);
					latch.await();
					index++;
				}
			}

			@Override
			public void cancel() {}
		});

		DataStream<Integer> extractOp = source1.assignTimestampsAndWatermarks(
				new AscendingTimestampExtractor<Integer>() {
					@Override
					public long extractAscendingTimestamp(Integer element) {
						return element;
					}
				});

		extractOp
				.transform("Watermark Check", BasicTypeInfo.INT_TYPE_INFO, new CustomOperator(true))
				.transform("Timestamp Check",
						BasicTypeInfo.INT_TYPE_INFO,
						new TimestampCheckingOperator());

		// verify that extractor picks up source parallelism
		Assert.assertEquals(extractOp.getTransformation().getParallelism(), source1.getTransformation().getParallelism());

		env.execute();

		// verify that we get NUM_ELEMENTS watermarks
		for (int j = 0; j < NUM_ELEMENTS; j++) {
			if (!CustomOperator.finalWatermarks[0].get(j).equals(new Watermark(j))) {
				long wm = CustomOperator.finalWatermarks[0].get(j).getTimestamp();
				Assert.fail("Wrong watermark. Expected: " + j + " Found: " + wm + " All: " + CustomOperator.finalWatermarks[0]);
			}
		}
		
		// the input is finite, so it should have a MAX Watermark
		assertEquals(Watermark.MAX_WATERMARK, 
				CustomOperator.finalWatermarks[0].get(CustomOperator.finalWatermarks[0].size() - 1));
	}

	/**
	 * This thests whether timestamps are properly extracted in the timestamp
	 * extractor and whether watermark are correctly forwarded from the custom watermark emit
	 * function.
	 */
	@Test
	public void testTimestampExtractorWithCustomWatermarkEmit() throws Exception {
		final int NUM_ELEMENTS = 10;

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.getConfig().setAutoWatermarkInterval(10);
		env.setParallelism(1);
		env.getConfig().disableSysoutLogging();


		DataStream<Integer> source1 = env.addSource(new SourceFunction<Integer>() {
			@Override
			public void run(SourceContext<Integer> ctx) throws Exception {
				int index = 1;
				while (index <= NUM_ELEMENTS) {
					ctx.collect(index);
					latch.await();
					index++;
				}
			}

			@Override
			public void cancel() {}
		});

		source1
				.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Integer>() {
					
					@Override
					public long extractTimestamp(Integer element, long currentTimestamp) {
						return element;
					}

					@Override
					public Watermark checkAndGetNextWatermark(Integer element, long extractedTimestamp) {
						return new Watermark(extractedTimestamp - 1);
					}
				})
				.transform("Watermark Check", BasicTypeInfo.INT_TYPE_INFO, new CustomOperator(true))
				.transform("Timestamp Check", BasicTypeInfo.INT_TYPE_INFO, new TimestampCheckingOperator());


		env.execute();

		// verify that we get NUM_ELEMENTS watermarks
		for (int j = 0; j < NUM_ELEMENTS; j++) {
			if (!CustomOperator.finalWatermarks[0].get(j).equals(new Watermark(j))) {
				Assert.fail("Wrong watermark.");
			}
		}

		// the input is finite, so it should have a MAX Watermark
		assertEquals(Watermark.MAX_WATERMARK,
				CustomOperator.finalWatermarks[0].get(CustomOperator.finalWatermarks[0].size() - 1));
	}

	/**
	 * This test verifies that the timestamp extractor does not emit decreasing watermarks even
	 *
	 */
	@Test
	public void testTimestampExtractorWithDecreasingCustomWatermarkEmit() throws Exception {
		final int NUM_ELEMENTS = 10;

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.getConfig().setAutoWatermarkInterval(1);
		env.setParallelism(1);
		env.getConfig().disableSysoutLogging();


		DataStream<Integer> source1 = env.addSource(new SourceFunction<Integer>() {
			@Override
			public void run(SourceContext<Integer> ctx) throws Exception {
				int index = 1;
				while (index <= NUM_ELEMENTS) {
					ctx.collect(index);
					Thread.sleep(100);
					ctx.collect(index - 1);
					latch.await();
					index++;
				}
			}

			@Override
			public void cancel() {}
		});

		source1
				.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Integer>() {

					@Override
					public long extractTimestamp(Integer element, long previousTimestamp) {
						return element;
					}

					@Override
					public Watermark checkAndGetNextWatermark(Integer element, long extractedTimestamp) {
						return new Watermark(extractedTimestamp - 1);
					}
				})
				.transform("Watermark Check", BasicTypeInfo.INT_TYPE_INFO, new CustomOperator(true))
				.transform("Timestamp Check", BasicTypeInfo.INT_TYPE_INFO, new TimestampCheckingOperator());


		env.execute();

		// verify that we get NUM_ELEMENTS watermarks
		for (int j = 0; j < NUM_ELEMENTS; j++) {
			if (!CustomOperator.finalWatermarks[0].get(j).equals(new Watermark(j))) {
				Assert.fail("Wrong watermark.");
			}
		}
		// the input is finite, so it should have a MAX Watermark
		assertEquals(Watermark.MAX_WATERMARK,
				CustomOperator.finalWatermarks[0].get(CustomOperator.finalWatermarks[0].size() - 1));
	}

	/**
	 * This test verifies that the timestamp extractor forwards Long.MAX_VALUE watermarks.
	 */
	@Test
	public void testTimestampExtractorWithLongMaxWatermarkFromSource() throws Exception {
		final int NUM_ELEMENTS = 10;

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.getConfig().setAutoWatermarkInterval(1);
		env.setParallelism(2);
		env.getConfig().disableSysoutLogging();


		DataStream<Integer> source1 = env.addSource(new SourceFunction<Integer>() {
			@Override
			public void run(SourceContext<Integer> ctx) throws Exception {
				int index = 1;
				while (index <= NUM_ELEMENTS) {
					ctx.collectWithTimestamp(index, index);
					ctx.collectWithTimestamp(index - 1, index - 1);
					index++;
					ctx.emitWatermark(new Watermark(index-2));
				}

				// emit the final Long.MAX_VALUE watermark, do it twice and verify that
				// we only see one in the result
				ctx.emitWatermark(new Watermark(Long.MAX_VALUE));
				ctx.emitWatermark(new Watermark(Long.MAX_VALUE));
			}

			@Override
			public void cancel() {}
		});

		source1
				.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Integer>() {

					@Override
					public long extractTimestamp(Integer element, long currentTimestamp) {
						return element;
					}

					@Override
					public Watermark checkAndGetNextWatermark(Integer element, long extractedTimestamp) {
						return null;
					}
				})
			.transform("Watermark Check", BasicTypeInfo.INT_TYPE_INFO, new CustomOperator(true));


		env.execute();

		Assert.assertTrue(CustomOperator.finalWatermarks[0].size() == 1);
		Assert.assertTrue(CustomOperator.finalWatermarks[0].get(0).getTimestamp() == Long.MAX_VALUE);
	}

	/**
	 * This test verifies that the timestamp extractor forwards Long.MAX_VALUE watermarks.
	 * 
	 * Same test as before, but using a different timestamp extractor
	 */
	@Test
	public void testTimestampExtractorWithLongMaxWatermarkFromSource2() throws Exception {
		final int NUM_ELEMENTS = 10;

		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.getConfig().setAutoWatermarkInterval(10);
		env.setParallelism(2);
		env.getConfig().disableSysoutLogging();

		DataStream<Integer> source1 = env.addSource(new SourceFunction<Integer>() {
			@Override
			public void run(SourceContext<Integer> ctx) throws Exception {
				int index = 1;
				while (index <= NUM_ELEMENTS) {
					ctx.collectWithTimestamp(index, index);
					ctx.collectWithTimestamp(index - 1, index - 1);
					index++;
					ctx.emitWatermark(new Watermark(index-2));
				}

				// emit the final Long.MAX_VALUE watermark, do it twice and verify that
				// we only see one in the result
				ctx.emitWatermark(new Watermark(Long.MAX_VALUE));
				ctx.emitWatermark(new Watermark(Long.MAX_VALUE));
			}

			@Override
			public void cancel() {}
		});

		source1
				.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Integer>() {

					@Override
					public long extractTimestamp(Integer element, long currentTimestamp) {
						return element;
					}

					@Override
					public Watermark getCurrentWatermark() {
						return null;
					}
				})
				.transform("Watermark Check", BasicTypeInfo.INT_TYPE_INFO, new CustomOperator(true));
		
		env.execute();

		Assert.assertTrue(CustomOperator.finalWatermarks[0].size() == 1);
		Assert.assertTrue(CustomOperator.finalWatermarks[0].get(0).getTimestamp() == Long.MAX_VALUE);
	}

	/**
	 * This verifies that an event time source works when setting stream time characteristic to
	 * processing time. In this case, the watermarks should just be swallowed.
	 */
	@Test
	public void testEventTimeSourceWithProcessingTime() throws Exception {
		StreamExecutionEnvironment env = 
				StreamExecutionEnvironment.getExecutionEnvironment();
		
		env.setParallelism(2);
		env.getConfig().disableSysoutLogging();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		DataStream<Integer> source1 = env.addSource(new MyTimestampSource(0, 10));

		source1
			.map(new IdentityMap())
			.transform("Watermark Check", BasicTypeInfo.INT_TYPE_INFO, new CustomOperator(false));

		env.execute();

		// verify that we don't get any watermarks, the source is used as watermark source in
		// other tests, so it normally emits watermarks
		Assert.assertTrue(CustomOperator.finalWatermarks[0].size() == 0);
	}
	
	@Test
	public void testErrorOnEventTimeOverProcessingTime() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(2);
		env.getConfig().disableSysoutLogging();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		DataStream<Tuple2<String, Integer>> source1 = 
				env.fromElements(new Tuple2<>("a", 1), new Tuple2<>("b", 2));

		source1
				.keyBy(0)
				.window(TumblingEventTimeWindows.of(Time.seconds(5)))
				.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
					@Override
					public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2)  {
						return value1;
					}
				})
				.print();

		try {
			env.execute();
			fail("this should fail with an exception");
		} catch (Exception e) {
			// expected
		}
	}

	@Test
	public void testErrorOnEventTimeWithoutTimestamps() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(2);
		env.getConfig().disableSysoutLogging();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<Tuple2<String, Integer>> source1 =
				env.fromElements(new Tuple2<>("a", 1), new Tuple2<>("b", 2));

		source1
				.keyBy(0)
				.window(TumblingEventTimeWindows.of(Time.seconds(5)))
				.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
					@Override
					public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2)  {
						return value1;
					}
				})
				.print();

		try {
			env.execute();
			fail("this should fail with an exception");
		} catch (Exception e) {
			// expected
		}
	}

	// ------------------------------------------------------------------------
	//  Custom Operators and Functions
	// ------------------------------------------------------------------------
	
	@SuppressWarnings("unchecked")
	public static class CustomOperator extends AbstractStreamOperator<Integer> implements OneInputStreamOperator<Integer, Integer> {

		List<Watermark> watermarks;
		public static List<Watermark>[] finalWatermarks = new List[PARALLELISM];
		private final boolean timestampsEnabled;

		public CustomOperator(boolean timestampsEnabled) {
			setChainingStrategy(ChainingStrategy.ALWAYS);
			this.timestampsEnabled = timestampsEnabled;
		}

		@Override
		public void processElement(StreamRecord<Integer> element) throws Exception {
			if (timestampsEnabled) {
				if (element.getTimestamp() != element.getValue()) {
					Assert.fail("Timestamps are not properly handled.");
				}
			}
			output.collect(element);
		}

		@Override
		public void processWatermark(Watermark mark) throws Exception {
			super.processWatermark(mark);

			for (Watermark previousMark: watermarks) {
				assertTrue(previousMark.getTimestamp() < mark.getTimestamp());
			}
			watermarks.add(mark);
			latch.trigger();
			output.emitWatermark(mark);
		}

		@Override
		public void open() throws Exception {
			super.open();
			watermarks = new ArrayList<>();
		}

		@Override
		public void close() throws Exception {
			super.close();
			finalWatermarks[getRuntimeContext().getIndexOfThisSubtask()] = watermarks;
		}
	}

	public static class TimestampCheckingOperator extends AbstractStreamOperator<Integer> implements OneInputStreamOperator<Integer, Integer> {

		public TimestampCheckingOperator() {
			setChainingStrategy(ChainingStrategy.ALWAYS);
		}

		@Override
		public void processElement(StreamRecord<Integer> element) throws Exception {
			if (element.getTimestamp() != element.getValue()) {
				Assert.fail("Timestamps are not properly handled.");
			}
			output.collect(element);
		}
	}

	public static class DisabledTimestampCheckingOperator extends AbstractStreamOperator<Integer> implements OneInputStreamOperator<Integer, Integer> {

		@Override
		public void processElement(StreamRecord<Integer> element) throws Exception {
			if (element.hasTimestamp()) {
				Assert.fail("Timestamps are not properly handled.");
			}
			output.collect(element);
		}
	}

	public static class IdentityCoMap implements CoMapFunction<Integer, Integer, Integer> {
		@Override
		public Integer map1(Integer value) throws Exception {
			return value;
		}

		@Override
		public Integer map2(Integer value) throws Exception {
			return value;
		}
	}

	public static class IdentityMap implements MapFunction<Integer, Integer> {
		@Override
		public Integer map(Integer value) throws Exception {
			return value;
		}
	}

	public static class MyTimestampSource implements SourceFunction<Integer> {

		private final long initialTime;
		private final int numWatermarks;

		public MyTimestampSource(long initialTime, int numWatermarks) {
			this.initialTime = initialTime;
			this.numWatermarks = numWatermarks;
		}

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			for (int i = 0; i < numWatermarks; i++) {
				ctx.collectWithTimestamp(i, initialTime + i);
				ctx.emitWatermark(new Watermark(initialTime + i));
			}
		}

		@Override
		public void cancel() {}
	}

	public static class MyTimestampSourceInfinite implements SourceFunction<Integer>, StoppableFunction {

		private final long initialTime;
		private final int numWatermarks;

		private volatile boolean running = true;
		
		public MyTimestampSourceInfinite(long initialTime, int numWatermarks) {
			this.initialTime = initialTime;
			this.numWatermarks = numWatermarks;
		}

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			for (int i = 0; i < numWatermarks; i++) {
				ctx.collectWithTimestamp(i, initialTime + i);
				ctx.emitWatermark(new Watermark(initialTime + i));
			}
			
			while (running) {
				Thread.sleep(20);
			}
		}

		@Override
		public void cancel() {
			running = false;
		}

		@Override
		public void stop() {
			running = false;
		}
	}

	public static class MyNonWatermarkingSource implements SourceFunction<Integer> {

		int numWatermarks;

		public MyNonWatermarkingSource(int numWatermarks) {
			this.numWatermarks = numWatermarks;
		}

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			for (int i = 0; i < numWatermarks; i++) {
				ctx.collect(i);
			}
		}

		@Override
		public void cancel() {}
	}
}
