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
import org.apache.flink.storm.tests.operators.TaskIdBolt;
import org.apache.flink.storm.util.BoltFileSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.MathUtils;

import org.apache.storm.Config;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * This test relies on the hash function used by the {@link DataStream#keyBy}, which is
 * assumed to be {@link MathUtils#murmurHash}.
 */
public class StormFieldsGroupingITCase extends AbstractTestBase {

	private static final String topologyId = "FieldsGrouping Test";
	private static final String spoutId = "spout";
	private static final String boltId = "bolt";
	private static final String sinkId = "sink";

	@Test
	public void testProgram() throws Exception {
		String resultPath = this.getTempDirPath("result");

		final String[] tokens = resultPath.split(":");
		final String outputFile = tokens[tokens.length - 1];

		final TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout(spoutId, new FiniteRandomSpout(0, 10, 2));
		builder.setBolt(boltId, new TaskIdBolt(), 2).fieldsGrouping(
				spoutId, FiniteRandomSpout.STREAM_PREFIX + 0, new Fields("number"));
		builder.setBolt(sinkId, new BoltFileSink(outputFile)).shuffleGrouping(boltId);

		final FlinkLocalCluster cluster = FlinkLocalCluster.getLocalCluster();
		Config conf = new Config();
		conf.put(FlinkLocalCluster.SUBMIT_BLOCKING, true); // only required to stabilize integration test
		cluster.submitTopology(topologyId, conf, FlinkTopology.createTopology(builder));
		cluster.shutdown();

		List<String> expectedResults = Arrays.asList(
			"-1155484576", "1033096058", "-1930858313", "1431162155", "-1557280266", "-1728529858", "1654374947",
			"-65105105", "-518907128", "-252332814");

		List<String> actualResults = new ArrayList<>();
		readAllResultLines(actualResults, resultPath, new String[0], false);

		//remove potential operator id prefix
		for (int i = 0; i < actualResults.size(); ++i) {
			String s = actualResults.get(i);
			if (s.contains(">")) {
				s = s.substring(s.indexOf(">") + 2);
				actualResults.set(i, s);
			}
		}

		Assert.assertEquals(expectedResults.size(), actualResults.size());
		Collections.sort(actualResults);
		Collections.sort(expectedResults);
		System.out.println(actualResults);
		for (int i = 0; i < actualResults.size(); ++i) {
			//compare against actual results with removed prefix (as it depends e.g. on the hash function used)
			Assert.assertEquals(expectedResults.get(i), actualResults.get(i));
		}
	}

}
