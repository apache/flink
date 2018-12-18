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

package org.apache.flink.storm.api;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.FlinkMiniCluster;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.streaming.api.graph.StreamGraph;

import org.apache.storm.LocalCluster;
import org.apache.storm.generated.ClusterSummary;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.RebalanceOptions;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.SubmitOptions;
import org.apache.storm.generated.TopologyInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;

/**
 * {@link FlinkLocalCluster} mimics a Storm {@link LocalCluster}.
 */
public class FlinkLocalCluster {

	/** The log used by this mini cluster. */
	private static final Logger LOG = LoggerFactory.getLogger(FlinkLocalCluster.class);

	/** The Flink mini cluster on which to execute the programs. */
	private FlinkMiniCluster flink;

	/** Configuration key to submit topology in blocking mode if flag is set to {@code true}. */
	public static final String SUBMIT_BLOCKING = "SUBMIT_STORM_TOPOLOGY_BLOCKING";

	public FlinkLocalCluster() {
	}

	public FlinkLocalCluster(FlinkMiniCluster flink) {
		this.flink = Objects.requireNonNull(flink);
	}

	@SuppressWarnings("rawtypes")
	public void submitTopology(final String topologyName, final Map conf, final FlinkTopology topology)
			throws Exception {
		this.submitTopologyWithOpts(topologyName, conf, topology, null);
	}

	@SuppressWarnings("rawtypes")
	public void submitTopologyWithOpts(final String topologyName, final Map conf, final FlinkTopology topology, final SubmitOptions submitOpts) throws Exception {
		LOG.info("Running Storm topology on FlinkLocalCluster");

		boolean submitBlocking = false;
		if (conf != null) {
			Object blockingFlag = conf.get(SUBMIT_BLOCKING);
			if (blockingFlag instanceof Boolean) {
				submitBlocking = ((Boolean) blockingFlag).booleanValue();
			}
		}

		FlinkClient.addStormConfigToTopology(topology, conf);

		StreamGraph streamGraph = topology.getExecutionEnvironment().getStreamGraph();
		streamGraph.setJobName(topologyName);

		JobGraph jobGraph = streamGraph.getJobGraph();

		if (this.flink == null) {
			Configuration configuration = new Configuration();
			configuration.addAll(jobGraph.getJobConfiguration());

			configuration.setString(TaskManagerOptions.MANAGED_MEMORY_SIZE, "0");
			configuration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, jobGraph.getMaximumParallelism());

			this.flink = new LocalFlinkMiniCluster(configuration, true);
			this.flink.start();
		}

		if (submitBlocking) {
			this.flink.submitJobAndWait(jobGraph, false);
		} else {
			this.flink.submitJobDetached(jobGraph);
		}
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
		if (this.flink != null) {
			this.flink.stop();
			this.flink = null;
		}
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

	// ------------------------------------------------------------------------
	//  Access to default local cluster
	// ------------------------------------------------------------------------

	// A different {@link FlinkLocalCluster} to be used for execution of ITCases
	private static LocalClusterFactory currentFactory = new DefaultLocalClusterFactory();

	/**
	 * Returns a {@link FlinkLocalCluster} that should be used for execution. If no cluster was set by
	 * {@link #initialize(LocalClusterFactory)} in advance, a new {@link FlinkLocalCluster} is returned.
	 *
	 * @return a {@link FlinkLocalCluster} to be used for execution
	 */
	public static FlinkLocalCluster getLocalCluster() {
		return currentFactory.createLocalCluster();
	}

	/**
	 * Sets a different factory for FlinkLocalClusters to be used for execution.
	 *
	 * @param clusterFactory
	 * 		The LocalClusterFactory to create the local clusters for execution.
	 */
	public static void initialize(LocalClusterFactory clusterFactory) {
		currentFactory = Objects.requireNonNull(clusterFactory);
	}

	// ------------------------------------------------------------------------
	//  Cluster factory
	// ------------------------------------------------------------------------

	/**
	 * A factory that creates local clusters.
	 */
	public interface LocalClusterFactory {

		/**
		 * Creates a local Flink cluster.
		 * @return A local Flink cluster.
		 */
		FlinkLocalCluster createLocalCluster();
	}

	/**
	 * A factory that instantiates a FlinkLocalCluster.
	 */
	public static class DefaultLocalClusterFactory implements LocalClusterFactory {

		@Override
		public FlinkLocalCluster createLocalCluster() {
			return new FlinkLocalCluster();
		}
	}
}
