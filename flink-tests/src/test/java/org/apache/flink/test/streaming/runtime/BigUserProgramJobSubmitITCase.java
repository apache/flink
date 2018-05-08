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
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;
import org.apache.flink.testutils.category.New;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * Integration test that verifies that a user program with a big(ger) payload is successfully
 * submitted and run.
 */
@Category(New.class)
public class BigUserProgramJobSubmitITCase extends TestLogger {

	// ------------------------------------------------------------------------
	//  The mini cluster that is shared across tests
	// ------------------------------------------------------------------------

	private static final MiniCluster CLUSTER;
	private static final RestClusterClient<StandaloneClusterId> CLIENT;

	static {
		try {
			MiniClusterConfiguration clusterConfiguration = new MiniClusterConfiguration.Builder()
				.setNumTaskManagers(1)
				.setNumSlotsPerTaskManager(1)
				.build();
			CLUSTER = new MiniCluster(clusterConfiguration);
			CLUSTER.start();

			URI restAddress = CLUSTER.getRestAddress();

			final Configuration clientConfig = new Configuration();
			clientConfig.setString(JobManagerOptions.ADDRESS, restAddress.getHost());
			clientConfig.setInteger(RestOptions.PORT, restAddress.getPort());

			CLIENT = new RestClusterClient<>(
				clientConfig,
				StandaloneClusterId.getInstance());

		} catch (Exception e) {
			throw new AssertionError("Could not setup cluster.", e);
		}
	}

	// ------------------------------------------------------------------------
	//  Cluster setup & teardown
	// ------------------------------------------------------------------------

	@AfterClass
	public static void teardown() throws Exception {
		CLIENT.shutdown();
		CLUSTER.close();
	}

	private final Random rnd = new Random();

	/**
	 * Use a map function that references a 100MB byte array.
	 */
	@Test
	public void bigDataInMap() throws Exception {

		final byte[] data = new byte[16 * 1024 * 1024]; // 16 MB
		rnd.nextBytes(data); // use random data so that Java does not optimise it away
		data[1] = 0;
		data[3] = 0;
		data[5] = 0;

		CollectingSink resultSink = new CollectingSink();

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		DataStream<Integer> src = env.fromElements(1, 3, 5);

		src.map(new MapFunction<Integer, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String map(Integer value) throws Exception {
				return "x " + value + " " + data[value];
			}
		}).addSink(resultSink);

		JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph());
		CLIENT.setDetached(false);
		CLIENT.submitJob(jobGraph, BigUserProgramJobSubmitITCase.class.getClassLoader());

		List<String> expected = Arrays.asList("x 1 0", "x 3 0", "x 5 0");

		List<String> result = CollectingSink.result;

		Collections.sort(expected);
		Collections.sort(result);

		assertEquals(expected, result);
	}

	private static class CollectingSink implements SinkFunction<String> {
		private static final List<String> result = Collections.synchronizedList(new ArrayList<>(3));

		public void invoke(String value, Context context) throws Exception {
			result.add(value);
		}
	}
}
