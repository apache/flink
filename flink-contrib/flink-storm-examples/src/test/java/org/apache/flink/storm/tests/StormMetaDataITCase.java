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

import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

import org.apache.flink.storm.api.FlinkLocalCluster;
import org.apache.flink.storm.api.FlinkTopology;
import org.apache.flink.storm.tests.operators.MetaDataSpout;
import org.apache.flink.storm.tests.operators.VerifyMetaDataBolt;
import org.apache.flink.streaming.util.StreamingProgramTestBase;
import org.junit.Assert;

public class StormMetaDataITCase extends StreamingProgramTestBase {

	private final static String topologyId = "FieldsGrouping Test";
	private final static String spoutId = "spout";
	private final static String boltId1 = "bolt1";
	private final static String boltId2 = "bolt2";

	@Override
	protected void testProgram() throws Exception {
		final TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout(spoutId, new MetaDataSpout(), 2);
		builder.setBolt(boltId1, new VerifyMetaDataBolt(), 2).localOrShuffleGrouping(spoutId,
				MetaDataSpout.STREAM_ID);
		builder.setBolt(boltId2, new VerifyMetaDataBolt()).shuffleGrouping(boltId1,
				VerifyMetaDataBolt.STREAM_ID);

		final FlinkLocalCluster cluster = FlinkLocalCluster.getLocalCluster();
		cluster.submitTopology(topologyId, null, FlinkTopology.createTopology(builder));

		// run topology for 5 seconds
		Utils.sleep(5 * 1000);

		cluster.shutdown();

		Assert.assertFalse(VerifyMetaDataBolt.errorOccured);
	}

}
