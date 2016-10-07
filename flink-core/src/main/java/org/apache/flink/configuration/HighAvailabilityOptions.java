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

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * The set of configuration options relating to high-availability settings.
 */
@PublicEvolving
public class HighAvailabilityOptions {

	// ------------------------------------------------------------------------
	//  Required High Availability Options
	// ------------------------------------------------------------------------

	/** 
	 * Defines high-availability mode used for the cluster execution.
	 * A value of "NONE" signals no highly available setup.
	 * To enable high-availability, set this mode to "ZOOKEEPER".
	 */
	public static final ConfigOption<String> HA_MODE = 
			key("high-availability")
			.defaultValue("NONE")
			.withDeprecatedKeys("recovery.mode");

	/**
	 * The ID of the Flink cluster, used to separate multiple Flink clusters 
	 * Needs to be set for standalone clusters, is automatically inferred in YARN and Mesos.
	 */
	public static final ConfigOption<String> HA_CLUSTER_ID = 
			key("high-availability.cluster-id")
			.defaultValue("/default")
			.withDeprecatedKeys("high-availability.zookeeper.path.namespace", "recovery.zookeeper.path.namespace");

	/**
	 * File system path (URI) where Flink persists metadata in high-availability setups
	 */
	public static final ConfigOption<String> HA_STORAGE_PATH =
			key("high-availability.storageDir")
			.noDefaultValue()
			.withDeprecatedKeys("high-availability.zookeeper.storageDir", "recovery.zookeeper.storageDir");

	/**
	 * The ZooKeeper quorum to use, when running Flink in a high-availability mode with ZooKeeper.
	 */
	public static final ConfigOption<String> HA_ZOOKEEPER_QUORUM =
			key("high-availability.zookeeper.quorum")
			.noDefaultValue()
			.withDeprecatedKeys("recovery.zookeeper.quorum");
	

	// ------------------------------------------------------------------------
	//  Recovery Options
	// ------------------------------------------------------------------------

	/**
	 * Optional port (range) used by the job manager in high-availability mode.
	 */
	public static final ConfigOption<String> HA_JOB_MANAGER_PORT_RANGE = 
			key("high-availability.jobmanager.port")
			.defaultValue("0")
			.withDeprecatedKeys("recovery.jobmanager.port");

	/**
	 * The time before a JobManager after a fail over recovers the current jobs.
	 */
	public static final ConfigOption<String> HA_JOB_DELAY = 
			key("high-availability.job.delay")
			.noDefaultValue()
			.withDeprecatedKeys("recovery.job.delay");

	// ------------------------------------------------------------------------
	//  ZooKeeper Options
	// ------------------------------------------------------------------------

	/**
	 * The root path under which Flink stores its entries in ZooKeeper
	 */
	public static final ConfigOption<String> HA_ZOOKEEPER_ROOT =
			key("high-availability.zookeeper.path.root")
			.defaultValue("/flink")
			.withDeprecatedKeys("recovery.zookeeper.path.root");

	// ------------------------------------------------------------------------
	//  ZooKeeper Client Settings
	// ------------------------------------------------------------------------

	public static final ConfigOption<Integer> ZOOKEEPER_SESSION_TIMEOUT = 
			key("high-availability.zookeeper.client.session-timeout")
			.defaultValue(60000)
			.withDeprecatedKeys("recovery.zookeeper.client.session-timeout");

	public static final ConfigOption<Integer> ZOOKEEPER_CONNECTION_TIMEOUT =
			key("high-availability.zookeeper.client.connection-timeout")
			.defaultValue(15000)
			.withDeprecatedKeys("recovery.zookeeper.client.connection-timeout");

	public static final ConfigOption<Integer> ZOOKEEPER_RETRY_WAIT = 
			key("high-availability.zookeeper.client.retry-wait")
			.defaultValue(5000)
			.withDeprecatedKeys("recovery.zookeeper.client.retry-wait");

	public static final ConfigOption<Integer> ZOOKEEPER_MAX_RETRY_ATTEMPTS = 
			key("high-availability.zookeeper.client.max-retry-attempts")
			.defaultValue(3)
			.withDeprecatedKeys("recovery.zookeeper.client.max-retry-attempts");

	public static final ConfigOption<Boolean> ZOOKEEPER_SASL_DISABLE = 
			key("zookeeper.sasl.disable")
			.defaultValue(true);

	public static final ConfigOption<String> ZOOKEEPER_SASL_SERVICE_NAME = 
			key("zookeeper.sasl.service-name")
			.noDefaultValue();

	// ------------------------------------------------------------------------

	/** Not intended to be instantiated */
	private HighAvailabilityOptions() {}
}
