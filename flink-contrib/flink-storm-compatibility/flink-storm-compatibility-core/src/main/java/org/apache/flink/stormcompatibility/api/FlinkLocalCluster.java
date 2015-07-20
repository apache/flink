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

package org.apache.flink.stormcompatibility.api;

import backtype.storm.LocalCluster;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.KillOptions;
import backtype.storm.generated.RebalanceOptions;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.SubmitOptions;
import backtype.storm.generated.TopologyInfo;
import org.apache.flink.streaming.util.ClusterUtil;

import java.util.Map;

/**
 * {@link FlinkLocalCluster} mimics a Storm {@link LocalCluster}.
 */
public class FlinkLocalCluster {

	public void submitTopology(final String topologyName, final Map<?, ?> conf, final FlinkTopology topology)
			throws Exception {
		this.submitTopologyWithOpts(topologyName, conf, topology, null);
	}

	public void submitTopologyWithOpts(final String topologyName, final Map<?, ?> conf, final FlinkTopology topology,
			final SubmitOptions submitOpts) throws Exception {
		ClusterUtil.startOnMiniCluster(topology.getStreamGraph().getJobGraph(topologyName), topology.getNumberOfTasks(), -1);
	}

	public void killTopology(final String topologyName) {
		this.killTopologyWithOpts(topologyName, null);
	}

	public void killTopologyWithOpts(final String name, final KillOptions options) {
	}

	public void activate(final String topologyName) {
	}

	public void deactivate(final String topologyName) {
	}

	public void rebalance(final String name, final RebalanceOptions options) {
	}

	public void shutdown() {
		ClusterUtil.stopOnMiniCluster();
	}

	public String getTopologyConf(final String id) {
		return null;
	}

	public StormTopology getTopology(final String id) {
		return null;
	}

	public ClusterSummary getClusterInfo() {
		return null;
	}

	public TopologyInfo getTopologyInfo(final String id) {
		return null;
	}

	public Map<?, ?> getState() {
		return null;
	}

	// A different {@link FlinkLocalCluster} to be used for execution of ITCases
	private static FlinkLocalCluster currentCluster = null;

	/**
	 * Returns a {@link FlinkLocalCluster} that should be used for execution. If no cluster was set by {@link
	 * #initialize(FlinkLocalCluster)} in advance, a new {@link FlinkLocalCluster} is returned.
	 *
	 * @return a {@link FlinkLocalCluster} to be used for execution
	 */
	public static FlinkLocalCluster getLocalCluster() {
		if (currentCluster == null) {
			currentCluster = new FlinkLocalCluster();
		}

		return currentCluster;
	}

	/**
	 * Sets a different {@link FlinkLocalCluster} to be used for execution.
	 *
	 * @param cluster
	 * 		the {@link FlinkLocalCluster} to be used for execution
	 */
	public static void initialize(final FlinkLocalCluster cluster) {
		currentCluster = cluster;
	}

}
