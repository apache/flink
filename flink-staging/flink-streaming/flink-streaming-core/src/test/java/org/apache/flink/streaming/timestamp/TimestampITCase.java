/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.timestamp;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.source.EventTimeSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.fail;

/**
 * Tests for timestamps, watermarks, and event-time sources.
 */
@SuppressWarnings("serial")
public class TimestampITCase {

	private static final int NUM_TASK_MANAGERS = 2;
	private static final int NUM_TASK_SLOTS = 3;
	private static final int PARALLELISM = NUM_TASK_MANAGERS * NUM_TASK_SLOTS;

	private static ForkableFlinkMiniCluster cluster;

	@BeforeClass
	public static void startCluster() {
		try {
			Configuration config = new Configuration();
			config.setInteger(ConfigConstants.LOCAL_INSTANCE_MANAGER_NUMBER_TASK_MANAGER, NUM_TASK_MANAGERS);
			config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, NUM_TASK_SLOTS);
			config.setString(ConfigConstants.DEFAULT_EXECUTION_RETRY_DELAY_KEY, "0 ms");
			config.setInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, 12);

			cluster = new ForkableFlinkMiniCluster(config, false);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail("Failed to start test cluster: " + e.getMessage());
		}
	}

	@AfterClass
	public static void shutdownCluster() {
		try {
			cluster.shutdown();
			cluster = null;
		}
		catch (Exception e) {
			e.printStackTrace();
			fail("Failed to stop test cluster: " + e.getMessage());
		}
	}

	/**
	 * These check whether custom timestamp emission works at sources and also whether timestamps
	 * arrive at operators throughout a topology.
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

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(
				"localhost", cluster.getJobManagerRPCPort());
		env.setParallelism(PARALLELISM);
		env.getConfig().disableSysoutLogging();
		env.getConfig().enableTimestamps();


		DataStream<Integer> source1 = env.addSource(new MyTimestampSource(initialTime, NUM_WATERMARKS));
		DataStream<Integer> source2 = env.addSource(new MyTimestampSource(initialTime, NUM_WATERMARKS));

		source1
				.map(new IdentityMap())
				.connect(source2).map(new IdentityCoMap())
				.transform("Custom Operator", BasicTypeInfo.INT_TYPE_INFO, new CustomOperator());

		env.execute();

		// verify that all the watermarks arrived at the final custom operator
		for (int i = 0; i < PARALLELISM; i++) {
			for (int j = 0; j < NUM_WATERMARKS; j++) {
				if (!CustomOperator.finalWatermarks[i].get(j).equals(new Watermark(initialTime + j))) {
					Assert.fail("Wrong watermark.");
				}
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


		StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(
				"localhost", cluster.getJobManagerRPCPort());
		env.setParallelism(PARALLELISM);
		env.getConfig().disableSysoutLogging();
		env.getConfig().enableTimestamps();


		DataStream<Integer> source1 = env.addSource(new MyTimestampSource(0L, NUM_ELEMENTS));
		DataStream<Integer> source2 = env.addSource(new MyTimestampSource(0L, NUM_ELEMENTS));

		source1
				.map(new IdentityMap())
				.connect(source2).map(new IdentityCoMap())
				.transform("Custom Operator", BasicTypeInfo.INT_TYPE_INFO, new TimestampCheckingOperator());

		env.execute();
	}

	/**
	 * These check whether timestamps are properly ignored when they are disabled.
	 */
	@Test
	public void testDisabledTimestamps() throws Exception {
		final int NUM_ELEMENTS = 10;


		StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(
				"localhost", cluster.getJobManagerRPCPort());
		env.setParallelism(PARALLELISM);
		env.getConfig().disableSysoutLogging();
		Assert.assertEquals("Timestamps are not disabled by default.", false, env.getConfig().areTimestampsEnabled());
		env.getConfig().disableTimestamps();


		DataStream<Integer> source1 = env.addSource(new MyNonWatermarkingSource(NUM_ELEMENTS));
		DataStream<Integer> source2 = env.addSource(new MyNonWatermarkingSource(NUM_ELEMENTS));

		source1
				.map(new IdentityMap())
				.connect(source2).map(new IdentityCoMap())
				.transform("Custom Operator", BasicTypeInfo.INT_TYPE_INFO, new DisabledTimestampCheckingOperator());

		env.execute();
	}

	/**
	 * This tests whether the program throws an exception when an event-time source tries
	 * to emit without timestamp.
	 */
	@Test(expected = ProgramInvocationException.class)
	public void testEventTimeSourceEmitWithoutTimestamp() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", cluster.getJobManagerRPCPort());
		env.setParallelism(PARALLELISM);
		env.getConfig().disableSysoutLogging();

		DataStream<Integer> source1 = env.addSource(new MyErroneousTimestampSource());

		source1
				.map(new IdentityMap());

		env.execute();
	}

	/**
	 * This tests whether the program throws an exception when a regular source tries
	 * to emit with timestamp.
	 */
	@Test(expected = ProgramInvocationException.class)
	public void testSourceEmitWithTimestamp() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", cluster.getJobManagerRPCPort());
		env.setParallelism(PARALLELISM);
		env.getConfig().disableSysoutLogging();

		DataStream<Integer> source1 = env.addSource(new MyErroneousSource());

		source1
				.map(new IdentityMap());

		env.execute();
	}

	/**
	 * This tests whether the program throws an exception when a regular source tries
	 * to emit a watermark.
	 */
	@Test(expected = ProgramInvocationException.class)
	public void testSourceEmitWatermark() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", cluster.getJobManagerRPCPort());
		env.setParallelism(PARALLELISM);
		env.getConfig().disableSysoutLogging();

		DataStream<Integer> source1 = env.addSource(new MyErroneousWatermarkSource());

		source1
				.map(new IdentityMap());

		env.execute();
	}

	@SuppressWarnings("unchecked")
	public static class CustomOperator extends AbstractStreamOperator<Integer> implements OneInputStreamOperator<Integer, Integer> {

		List<Watermark> watermarks;
		public static List<Watermark>[] finalWatermarks = new List[PARALLELISM];
		private long oldTimestamp;

		@Override
		public void processElement(StreamRecord<Integer> element) throws Exception {
			if (element.getTimestamp() != element.getValue()) {
				Assert.fail("Timestamps are not properly handled.");
			}
			oldTimestamp = element.getTimestamp();
			output.collect(element);
		}

		@Override
		public void processWatermark(Watermark mark) throws Exception {
			watermarks.add(mark);
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			watermarks = new ArrayList<Watermark>();
		}

		@Override
		public void close() throws Exception {
			super.close();
			finalWatermarks[getRuntimeContext().getIndexOfThisSubtask()] = watermarks;
		}
	}

	public static class TimestampCheckingOperator extends AbstractStreamOperator<Integer> implements OneInputStreamOperator<Integer, Integer> {

		@Override
		public void processElement(StreamRecord<Integer> element) throws Exception {
			if (element.getTimestamp() != element.getValue()) {
				Assert.fail("Timestamps are not properly handled.");
			}
			output.collect(element);
		}

		@Override
		public void processWatermark(Watermark mark) throws Exception {
		}
	}

	public static class DisabledTimestampCheckingOperator extends AbstractStreamOperator<Integer> implements OneInputStreamOperator<Integer, Integer> {

		@Override
		public void processElement(StreamRecord<Integer> element) throws Exception {
			if (element.getTimestamp() != 0) {
				Assert.fail("Timestamps are not properly handled.");
			}
			output.collect(element);
		}

		@Override
		public void processWatermark(Watermark mark) throws Exception {
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

	public static class MyTimestampSource implements EventTimeSourceFunction<Integer> {

		long initialTime;
		int numWatermarks;

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
		public void cancel() {

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
		public void cancel() {

		}
	}

	// This is a event-time source. This should only emit elements with timestamps. The test should
	// therefore throw an exception
	public static class MyErroneousTimestampSource implements EventTimeSourceFunction<Integer> {

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			for (int i = 0; i < 10; i++) {
				ctx.collect(i);
			}
		}

		@Override
		public void cancel() {

		}
	}

	// This is a normal source. This should only emit elements without timestamps. The test should
	// therefore throw an exception
	public static class MyErroneousSource implements SourceFunction<Integer> {

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			for (int i = 0; i < 10; i++) {
				ctx.collectWithTimestamp(i, 0L);
			}
		}

		@Override
		public void cancel() {

		}
	}

	// This is a normal source. This should only emit elements without timestamps. This also
	// must not emit watermarks. The test should therefore throw an exception
	public static class MyErroneousWatermarkSource implements SourceFunction<Integer> {

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			for (int i = 0; i < 10; i++) {
				ctx.collect(i);
				ctx.emitWatermark(new Watermark(0L));
			}
		}

		@Override
		public void cancel() {

		}
	}
}
