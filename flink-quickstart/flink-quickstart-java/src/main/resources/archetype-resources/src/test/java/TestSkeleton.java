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

package ${package};


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;
import org.apache.flink.test.util.SuccessException;
import org.apache.flink.test.util.TestUtils;
import org.apache.flink.util.Collector;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Skeleton class showing how to write integration tests against a running embedded Flink
 * testing cluster.
 */
public class TestSkeleton {

	protected static final Logger LOG = LoggerFactory.getLogger(TestSkeleton.class);
	private static ForkableFlinkMiniCluster flink;
	private static int flinkPort;

	/**
	 * Start a re-usable Flink mini cluster
	 */
	@BeforeClass
	public static void setupFlink() {
		Configuration flinkConfig = new Configuration();
		flinkConfig.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1);
		flinkConfig.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 8);
		flinkConfig.setInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, 16);
		flinkConfig.setString(ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_DELAY, "0 s");

		flink = new ForkableFlinkMiniCluster(flinkConfig, false);
		flink.start();

		flinkPort = flink.getLeaderRPCPort();
		LOG.info("Started a Flink testing cluster on port {}", flinkPort);
	}


	/**
	 * This test uses a finite stream. The TestingSource will stop after 1000 elements have been send.
	 * We use a user defined method to count the number of elements. After the execute() call,
	 * we check if the counter has the expected value.
	 *
	 * Note that this method of using a static variable for accessing state from a user defined method
	 * doesn't work on clusters.
	 */
	final static Tuple1<Long> counter = new Tuple1<>(0L);
	@Test
	public void testFiniteStreamingJob() throws Exception {
		StreamExecutionEnvironment see = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);

		// create stream with 1000 elements
		DataStream<String> elements = see.addSource(new TestingSource(1000));

		elements.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public void flatMap(String s, Collector<String> collector) throws Exception {
				// count elements in stream
				counter.f0++;
			}
		});
		see.execute("Finite Streaming Job");

		Assert.assertEquals("Wrong element count", 1000L, (long)counter.f0);
	}

	/**
	 * This test shows how to start an infinitely running streaming job.
	 *
	 * With the SuccessException() / tryExecute() method, we can stop the streaming job from within
	 * user defined functions.
	 *
	 * @throws Exception
	 */
	@Test
	public void testInfiniteStream() throws Exception {
		StreamExecutionEnvironment see = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);

		// create infinite stream
		DataStream<String> elements = see.addSource(new TestingSource(-1));
		elements.flatMap(new FlatMapFunction<String, String>() {
			private long count = 0;
			@Override
			public void flatMap(String s, Collector<String> collector) throws Exception {
				if(count++ == 10_000) {
					// intentionally fail infinite job by throwing a SuccessException.
					throw new SuccessException();
				}
			}
		});

		// filters SuccessException.
		TestUtils.tryExecute(see, "infinite stream");
	}

	/**
	 * This test shows how the TestUtils.tryExecute() method forwards runtime exceptions to the test.
	 *
	 * @throws Exception
	 */
	@Test(expected = AssertionError.class)
	public void testFailureStream() throws Exception {
		StreamExecutionEnvironment see = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);

		// create infinite stream
		DataStream<String> elements = see.addSource(new TestingSource(-1));
		elements.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public void flatMap(String s, Collector<String> collector) throws Exception {
				// for the infinite TestingSource, the count will be negative in the beginning
				Assert.assertTrue(Long.parseLong(s.split("-")[1]) > 0);
				// this will never be thrown
				throw new SuccessException();
			}
		});

		// filters SuccessException.
		TestUtils.tryExecute(see, "infinite failing stream");
	}

	/**
	 * Stop Flink cluster after the tests were executed
	 */
	@AfterClass
	public static void teardownFlink() {
		if (flink != null) {
			flink.shutdown();
			LOG.info("Stopped Flink testing cluster");
		}
	}

	/**
	 * Utility data source (non-parallel)
	 */
	private class TestingSource implements SourceFunction<String> {
		private long elements;
		private boolean running = true;

		public TestingSource(long elements) {
			this.elements = elements;
		}

		@Override
		public void run(SourceContext<String> sourceContext) throws Exception {
			while(elements-- != 0 && running ) {
				sourceContext.collect("element-"+elements);
			}
		}

		@Override
		public void cancel() {
			this.running = false;
		}
	}
}
