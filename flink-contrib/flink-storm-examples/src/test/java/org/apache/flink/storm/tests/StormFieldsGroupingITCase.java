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

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import org.apache.flink.runtime.util.MathUtils;
import org.apache.flink.storm.api.FlinkLocalCluster;
import org.apache.flink.storm.api.FlinkTopology;
import org.apache.flink.storm.tests.operators.FiniteRandomSpout;
import org.apache.flink.storm.tests.operators.TaskIdBolt;
import org.apache.flink.storm.util.BoltFileSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.util.StreamingProgramTestBase;

/**
 * This test relies on the hash function used by the {@link DataStream#keyBy}, which is
 * assumed to be {@link MathUtils#murmurHash}.
 */
public class StormFieldsGroupingITCase extends StreamingProgramTestBase {

	private final static String topologyId = "FieldsGrouping Test";
	private final static String spoutId = "spout";
	private final static String boltId = "bolt";
	private final static String sinkId = "sink";
	private String resultPath;

	@Override
	protected void preSubmit() throws Exception {
		this.resultPath = this.getTempDirPath("result");
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory("3> -1155484576\n" + "3> 1033096058\n" + "3> -1930858313\n" +
			"3> 1431162155\n" + "4> -1557280266\n" + "4> -1728529858\n" + "4> 1654374947\n" +
			"4> -65105105\n" + "4> -518907128\n" + "4> -252332814\n", this.resultPath);
	}

	@Override
	protected void testProgram() throws Exception {
		final String[] tokens = this.resultPath.split(":");
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
	}

}
