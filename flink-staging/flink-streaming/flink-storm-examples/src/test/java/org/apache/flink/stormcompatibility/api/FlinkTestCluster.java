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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.TestStreamEnvironment;

import java.util.Map;

/**
 * {@link FlinkTestCluster} mimics a Storm {@link LocalCluster} for ITCases via a {@link TestStreamEnvironment}.
 */
public class FlinkTestCluster extends FlinkLocalCluster {

	@Override
	public void submitTopology(final String topologyName, final Map<?, ?> conf, final FlinkTopology topology)
			throws Exception {
		this.submitTopologyWithOpts(topologyName, conf, topology, null);
	}

	@Override
	public void submitTopologyWithOpts(final String topologyName, final Map<?, ?> conf, final FlinkTopology topology,
			final SubmitOptions submitOpts)
			throws Exception {
		final TestStreamEnvironment env = (TestStreamEnvironment) StreamExecutionEnvironment.getExecutionEnvironment();
		env.start(topology.getStreamGraph().getJobGraph(topologyName));
	}

	@Override
	public void killTopology(final String topologyName) {
	}

	@Override
	public void killTopologyWithOpts(final String name, final KillOptions options) {
	}

	@Override
	public void activate(final String topologyName) {
	}

	@Override
	public void deactivate(final String topologyName) {
	}

	@Override
	public void rebalance(final String name, final RebalanceOptions options) {
	}

	@Override
	public void shutdown() {
		final TestStreamEnvironment env = (TestStreamEnvironment) StreamExecutionEnvironment.getExecutionEnvironment();
		try {
			env.shutdown();
		} catch (final InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public String getTopologyConf(final String id) {
		return null;
	}

	@Override
	public StormTopology getTopology(final String id) {
		return null;
	}

	@Override
	public ClusterSummary getClusterInfo() {
		return null;
	}

	@Override
	public TopologyInfo getTopologyInfo(final String id) {
		return null;
	}

	@Override
	public Map<?, ?> getState() {
		return null;
	}

}
