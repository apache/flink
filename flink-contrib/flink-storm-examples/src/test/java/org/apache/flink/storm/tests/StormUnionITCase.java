/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.storm.tests;

import org.apache.flink.storm.api.FlinkLocalCluster;
import org.apache.flink.storm.api.FlinkTopology;
import org.apache.flink.storm.tests.operators.FiniteRandomSpout;
import org.apache.flink.storm.tests.operators.MergerBolt;
import org.apache.flink.storm.util.BoltFileSink;
import org.apache.flink.test.util.AbstractTestBase;

import org.apache.storm.Config;
import org.apache.storm.topology.TopologyBuilder;
import org.junit.Test;

/**
 * Test for the {@link MergerBolt}.
 */
public class StormUnionITCase extends AbstractTestBase {

	private static final String RESULT = "-1154715079\n" + "-1155869325\n" + "-1155484576\n"
			+ "431529176\n" + "1260042744\n" + "1761283695\n" + "1749940626\n" + "892128508\n"
			+ "155629808\n" + "1429008869\n" + "-1465154083\n" + "-723955400\n" + "-423279216\n"
			+ "17850135\n" + "2133836778\n" + "1033096058\n" + "-1690734402\n" + "-1557280266\n"
			+ "1327362106\n" + "-1930858313\n" + "502539523\n" + "-1728529858\n" + "-938301587\n"
			+ "-624140595\n" + "-60658084\n" + "142959438\n" + "-613647601\n" + "-330177159\n"
			+ "-54027108\n" + "1945002173\n" + "979930868";

	private static final String topologyId = "Multiple Input Streams Test";
	private static final String spoutId1 = "spout1";
	private static final String spoutId2 = "spout2";
	private static final String spoutId3 = "spout3";
	private static final String boltId = "merger";
	private static final String sinkId = "sink";

	@Test
	public void testProgram() throws Exception {
		String resultPath = this.getTempDirPath("result");

		final TopologyBuilder builder = new TopologyBuilder();

		// get input data
		builder.setSpout(spoutId1, new FiniteRandomSpout(0, 10));
		builder.setSpout(spoutId2, new FiniteRandomSpout(1, 8));
		builder.setSpout(spoutId3, new FiniteRandomSpout(2, 13));

		builder.setBolt(boltId, new MergerBolt())
				.shuffleGrouping(spoutId1, FiniteRandomSpout.STREAM_PREFIX + 0)
				.shuffleGrouping(spoutId2, FiniteRandomSpout.STREAM_PREFIX + 0)
				.shuffleGrouping(spoutId3, FiniteRandomSpout.STREAM_PREFIX + 0);

		final String[] tokens = resultPath.split(":");
		final String outputFile = tokens[tokens.length - 1];
		builder.setBolt(sinkId, new BoltFileSink(outputFile)).shuffleGrouping(boltId);

		// execute program locally
		final FlinkLocalCluster cluster = FlinkLocalCluster.getLocalCluster();
		Config conf = new Config();
		conf.put(FlinkLocalCluster.SUBMIT_BLOCKING, true); // only required to stabilize integration test
		cluster.submitTopology(topologyId, conf, FlinkTopology.createTopology(builder));
		cluster.shutdown();

		compareResultsByLinesInMemory(RESULT, resultPath);
	}

}
