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

package org.apache.flink.test.streaming.runtime;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.test.streaming.runtime.util.TestListResultSink;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * Integration test that verifies that a user program with a big(ger) payload is successfully
 * submitted and run.
 */
@Ignore("Fails on job submission payload being too large - [FLINK-7285]")
public class BigUserProgramJobSubmitITCase extends TestLogger {

	// ------------------------------------------------------------------------
	//  The mini cluster that is shared across tests
	// ------------------------------------------------------------------------

	private static final int DEFAULT_PARALLELISM = 1;

	private static LocalFlinkMiniCluster cluster;

	private static final Logger LOG = LoggerFactory.getLogger(BigUserProgramJobSubmitITCase.class);

	// ------------------------------------------------------------------------
	//  Cluster setup & teardown
	// ------------------------------------------------------------------------

	@BeforeClass
	public static void setup() throws Exception {
		// make sure we do not use a singleActorSystem for the tests
		// (therefore, we cannot simply inherit from StreamingMultipleProgramsTestBase)
		LOG.info("Starting FlinkMiniCluster");
		cluster = TestBaseUtils.startCluster(1, DEFAULT_PARALLELISM, false, false, false);
		TestStreamEnvironment.setAsContext(cluster, DEFAULT_PARALLELISM);
	}

	@AfterClass
	public static void teardown() throws Exception {
		LOG.info("Closing FlinkMiniCluster");
		TestStreamEnvironment.unsetAsContext();
		TestBaseUtils.stopCluster(cluster, TestBaseUtils.DEFAULT_TIMEOUT);
	}

	private final Random rnd = new Random();

	/**
	 * Use a map function that references a 100MB byte array.
	 */
	@Test
	public void bigDataInMap() throws Exception {

		final byte[] data = new byte[100 * 1024 * 1024]; // 100 MB
		rnd.nextBytes(data); // use random data so that Java does not optimise it away
		data[1] = 0;
		data[3] = 0;
		data[5] = 0;

		TestListResultSink<String> resultSink = new TestListResultSink<>();

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Integer> src = env.fromElements(1, 3, 5);

		src.map(new MapFunction<Integer, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String map(Integer value) throws Exception {
				return "x " + value + " " + data[value];
			}
		}).addSink(resultSink);

		env.execute();

		List<String> expected = Arrays.asList("x 1 0", "x 3 0", "x 5 0");

		List<String> result = resultSink.getResult();

		Collections.sort(expected);
		Collections.sort(result);

		assertEquals(expected, result);
	}
}
