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

package org.apache.flink.configuration;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.ConfigGroup;
import org.apache.flink.annotation.docs.ConfigGroups;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.description.Description;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * The set of configuration options relating to high-availability settings.
 */
@PublicEvolving
@ConfigGroups(groups = {
	@ConfigGroup(name = "HighAvailabilityZookeeper", keyPrefix = "high-availability.zookeeper")
})
public class HighAvailabilityOptions {

	// ------------------------------------------------------------------------
	//  Required High Availability Options
	// ------------------------------------------------------------------------

	/**
	 * Defines high-availability mode used for the cluster execution.
	 * A value of "NONE" signals no highly available setup.
	 * To enable high-availability, set this mode to "ZOOKEEPER".
	 * Can also be set to FQN of HighAvailability factory class.
	 */
	@Documentation.CommonOption(position = Documentation.CommonOption.POSITION_HIGH_AVAILABILITY)
	public static final ConfigOption<String> HA_MODE =
			key("high-availability")
			.defaultValue("NONE")
			.withDeprecatedKeys("recovery.mode")
			.withDescription("Defines high-availability mode used for the cluster execution." +
				" To enable high-availability, set this mode to \"ZOOKEEPER\" or specify FQN of factory class.");

	/**
	 * The ID of the Flink cluster, used to separate multiple Flink clusters
	 * Needs to be set for standalone clusters, is automatically inferred in YARN and Mesos.
	 */
	public static final ConfigOption<String> HA_CLUSTER_ID =
			key("high-availability.cluster-id")
			.defaultValue("/default")
			.withDeprecatedKeys("high-availability.zookeeper.path.namespace", "recovery.zookeeper.path.namespace")
			.withDescription("The ID of the Flink cluster, used to separate multiple Flink clusters from each other." +
				" Needs to be set for standalone clusters but is automatically inferred in YARN and Mesos.");

	/**
	 * File system path (URI) where Flink persists metadata in high-availability setups.
	 */
	@Documentation.CommonOption(position = Documentation.CommonOption.POSITION_HIGH_AVAILABILITY)
	public static final ConfigOption<String> HA_STORAGE_PATH =
			key("high-availability.storageDir")
			.noDefaultValue()
			.withDeprecatedKeys("high-availability.zookeeper.storageDir", "recovery.zookeeper.storageDir")
			.withDescription("File system path (URI) where Flink persists metadata in high-availability setups.");

	// ------------------------------------------------------------------------
	//  Recovery Options
	// ------------------------------------------------------------------------

	/**
	 * Optional port (range) used by the job manager in high-availability mode.
	 */
	public static final ConfigOption<String> HA_JOB_MANAGER_PORT_RANGE =
			key("high-availability.jobmanager.port")
			.defaultValue("0")
			.withDeprecatedKeys("recovery.jobmanager.port")
			.withDescription("Optional port (range) used by the job manager in high-availability mode.");

	/**
	 * The time before a JobManager after a fail over recovers the current jobs.
	 */
	public static final ConfigOption<String> HA_JOB_DELAY =
			key("high-availability.job.delay")
			.noDefaultValue()
			.withDeprecatedKeys("recovery.job.delay")
			.withDescription("The time before a JobManager after a fail over recovers the current jobs.");

	// ------------------------------------------------------------------------
	//  ZooKeeper Options
	// ------------------------------------------------------------------------

	/**
	 * The ZooKeeper quorum to use, when running Flink in a high-availability mode with ZooKeeper.
	 */
	public static final ConfigOption<String> HA_ZOOKEEPER_QUORUM =
			key("high-availability.zookeeper.quorum")
			.noDefaultValue()
			.withDeprecatedKeys("recovery.zookeeper.quorum")
			.withDescription("The ZooKeeper quorum to use, when running Flink in a high-availability mode with ZooKeeper.");

	/**
	 * The root path under which Flink stores its entries in ZooKeeper.
	 */
	public static final ConfigOption<String> HA_ZOOKEEPER_ROOT =
			key("high-availability.zookeeper.path.root")
			.defaultValue("/flink")
			.withDeprecatedKeys("recovery.zookeeper.path.root")
			.withDescription("The root path under which Flink stores its entries in ZooKeeper.");

	public static final ConfigOption<String> HA_ZOOKEEPER_LATCH_PATH =
			key("high-availability.zookeeper.path.latch")
			.defaultValue("/leaderlatch")
			.withDeprecatedKeys("recovery.zookeeper.path.latch")
			.withDescription("Defines the znode of the leader latch which is used to elect the leader.");

	/** ZooKeeper root path (ZNode) for job graphs. */
	public static final ConfigOption<String> HA_ZOOKEEPER_JOBGRAPHS_PATH =
			key("high-availability.zookeeper.path.jobgraphs")
			.defaultValue("/jobgraphs")
			.withDeprecatedKeys("recovery.zookeeper.path.jobgraphs")
			.withDescription("ZooKeeper root path (ZNode) for job graphs");

	public static final ConfigOption<String> HA_ZOOKEEPER_LEADER_PATH =
			key("high-availability.zookeeper.path.leader")
			.defaultValue("/leader")
			.withDeprecatedKeys("recovery.zookeeper.path.leader")
			.withDescription("Defines the znode of the leader which contains the URL to the leader and the current" +
				" leader session ID.");

	/** ZooKeeper root path (ZNode) for completed checkpoints. */
	public static final ConfigOption<String> HA_ZOOKEEPER_CHECKPOINTS_PATH =
			key("high-availability.zookeeper.path.checkpoints")
			.defaultValue("/checkpoints")
			.withDeprecatedKeys("recovery.zookeeper.path.checkpoints")
			.withDescription("ZooKeeper root path (ZNode) for completed checkpoints.");

	/** ZooKeeper root path (ZNode) for checkpoint counters. */
	public static final ConfigOption<String> HA_ZOOKEEPER_CHECKPOINT_COUNTER_PATH =
			key("high-availability.zookeeper.path.checkpoint-counter")
			.defaultValue("/checkpoint-counter")
			.withDeprecatedKeys("recovery.zookeeper.path.checkpoint-counter")
			.withDescription("ZooKeeper root path (ZNode) for checkpoint counters.");

	/** ZooKeeper root path (ZNode) for Mesos workers. */
	@PublicEvolving
	public static final ConfigOption<String> HA_ZOOKEEPER_MESOS_WORKERS_PATH =
			key("high-availability.zookeeper.path.mesos-workers")
			.defaultValue("/mesos-workers")
			.withDeprecatedKeys("recovery.zookeeper.path.mesos-workers")
			.withDescription(Description.builder()
				.text("The ZooKeeper root path for persisting the Mesos worker information.")
				.build());

	// ------------------------------------------------------------------------
	//  ZooKeeper Client Settings
	// ------------------------------------------------------------------------

	public static final ConfigOption<Integer> ZOOKEEPER_SESSION_TIMEOUT =
			key("high-availability.zookeeper.client.session-timeout")
			.defaultValue(60000)
			.withDeprecatedKeys("recovery.zookeeper.client.session-timeout")
			.withDescription("Defines the session timeout for the ZooKeeper session in ms.");

	public static final ConfigOption<Integer> ZOOKEEPER_CONNECTION_TIMEOUT =
			key("high-availability.zookeeper.client.connection-timeout")
			.defaultValue(15000)
			.withDeprecatedKeys("recovery.zookeeper.client.connection-timeout")
			.withDescription("Defines the connection timeout for ZooKeeper in ms.");

	public static final ConfigOption<Integer> ZOOKEEPER_RETRY_WAIT =
			key("high-availability.zookeeper.client.retry-wait")
			.defaultValue(5000)
			.withDeprecatedKeys("recovery.zookeeper.client.retry-wait")
			.withDescription("Defines the pause between consecutive retries in ms.");

	public static final ConfigOption<Integer> ZOOKEEPER_MAX_RETRY_ATTEMPTS =
			key("high-availability.zookeeper.client.max-retry-attempts")
			.defaultValue(3)
			.withDeprecatedKeys("recovery.zookeeper.client.max-retry-attempts")
			.withDescription("Defines the number of connection retries before the client gives up.");

	public static final ConfigOption<String> ZOOKEEPER_RUNNING_JOB_REGISTRY_PATH =
			key("high-availability.zookeeper.path.running-registry")
			.defaultValue("/running_job_registry/");

	public static final ConfigOption<String> ZOOKEEPER_CLIENT_ACL =
			key("high-availability.zookeeper.client.acl")
			.defaultValue("open")
			.withDescription("Defines the ACL (open|creator) to be configured on ZK node. The configuration value can be" +
				" set to “creator” if the ZooKeeper server configuration has the “authProvider” property mapped to use" +
				" SASLAuthenticationProvider and the cluster is configured to run in secure mode (Kerberos).");

	// ------------------------------------------------------------------------

	/** Not intended to be instantiated. */
	private HighAvailabilityOptions() {}
}
