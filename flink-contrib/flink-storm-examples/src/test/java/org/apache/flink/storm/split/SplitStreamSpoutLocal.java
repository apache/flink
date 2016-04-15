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
package org.apache.flink.storm.split;

import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import org.apache.flink.storm.api.FlinkLocalCluster;
import org.apache.flink.storm.api.FlinkTopology;

public class SplitStreamSpoutLocal {
	public final static String topologyId = "Spout split stream example";

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(final String[] args) throws Exception {

		if (!SplitSpoutTopology.parseParameters(args)) {
			return;
		}

		// build Topology the Storm way
		final TopologyBuilder builder = SplitSpoutTopology.buildTopology();

		final FlinkLocalCluster cluster = FlinkLocalCluster.getLocalCluster();
		cluster.submitTopology(topologyId, null, FlinkTopology.createTopology(builder));

		// run topology for 5 seconds
		Utils.sleep(5 * 1000);

		cluster.shutdown();
	}

}
