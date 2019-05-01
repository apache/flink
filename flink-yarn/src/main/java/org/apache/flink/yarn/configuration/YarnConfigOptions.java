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

package org.apache.flink.yarn.configuration;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.description.Description;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.LinkElement.link;
import static org.apache.flink.configuration.description.TextElement.code;
import static org.apache.flink.configuration.description.TextElement.text;

/**
 * This class holds configuration constants used by Flink's YARN runners.
 *
 * <p>These options are not expected to be ever configured by users explicitly.
 */
public class YarnConfigOptions {

	/**
	 * The hostname or address where the application master RPC system is listening.
	 */
	public static final ConfigOption<String> APP_MASTER_RPC_ADDRESS =
			key("yarn.appmaster.rpc.address")
			.noDefaultValue()
			.withDescription("The hostname or address where the application master RPC system is listening.");

	/**
	 * The port where the application master RPC system is listening.
	 */
	public static final ConfigOption<Integer> APP_MASTER_RPC_PORT =
			key("yarn.appmaster.rpc.port")
			.defaultValue(-1)
			.withDescription("The port where the application master RPC system is listening.");

	/**
	 * Defines whether user-jars are included in the system class path for per-job-clusters as well as their positioning
	 * in the path. They can be positioned at the beginning ("FIRST"), at the end ("LAST"), or be positioned based on
	 * their name ("ORDER").
	 */
	public static final ConfigOption<String> CLASSPATH_INCLUDE_USER_JAR =
		key("yarn.per-job-cluster.include-user-jar")
			.defaultValue("ORDER")
			.withDescription("Defines whether user-jars are included in the system class path for per-job-clusters as" +
				" well as their positioning in the path. They can be positioned at the beginning (\"FIRST\"), at the" +
				" end (\"LAST\"), or be positioned based on their name (\"ORDER\"). Setting this parameter to" +
				" \"DISABLED\" causes the jar to be included in the user class path instead.");

	/**
	 * The vcores exposed by YARN.
	 */
	public static final ConfigOption<Integer> VCORES =
		key("yarn.containers.vcores")
			.defaultValue(-1)
			.withDescription(Description.builder().text(
					"The number of virtual cores (vcores) per YARN container. By default, the number of vcores" +
					" is set to the number of slots per TaskManager, if set, or to 1, otherwise. In order for this" +
					" parameter to be used your cluster must have CPU scheduling enabled. You can do this by setting" +
					" the %s.",
				code("org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler"))
				.build());

	/**
	 * The maximum number of failed YARN containers before entirely stopping
	 * the YARN session / job on YARN.
	 * By default, we take the number of initially requested containers.
	 *
	 * <p>Note: This option returns a String since Integer options must have a static default value.
	 */
	public static final ConfigOption<String> MAX_FAILED_CONTAINERS =
		key("yarn.maximum-failed-containers")
		.noDefaultValue()
		.withDescription("Maximum number of containers the system is going to reallocate in case of a failure.");

	/**
	 * Set the number of retries for failed YARN ApplicationMasters/JobManagers in high
	 * availability mode. This value is usually limited by YARN.
	 * By default, it's 1 in the standalone case and 2 in the high availability case.
	 *
	 * <p>>Note: This option returns a String since Integer options must have a static default value.
	 */
	public static final ConfigOption<String> APPLICATION_ATTEMPTS =
		key("yarn.application-attempts")
		.noDefaultValue()
		.withDescription("Number of ApplicationMaster restarts. Note that that the entire Flink cluster will restart" +
			" and the YARN Client will loose the connection. Also, the JobManager address will change and you’ll need" +
			" to set the JM host:port manually. It is recommended to leave this option at 1.");

	/**
	 * The heartbeat interval between the Application Master and the YARN Resource Manager.
	 */
	public static final ConfigOption<Integer> HEARTBEAT_DELAY_SECONDS =
		key("yarn.heartbeat-delay")
		.defaultValue(5)
		.withDescription("Time between heartbeats with the ResourceManager in seconds.");

	/**
	 * The heartbeat interval between the Application Master and the YARN Resource Manager
	 * if Flink is requesting containers.
	 */
	public static final ConfigOption<Integer> CONTAINER_REQUEST_HEARTBEAT_INTERVAL_MILLISECONDS =
		key("yarn.heartbeat.container-request-interval")
			.defaultValue(500)
			.withDescription(
				new Description.DescriptionBuilder()
					.text("Time between heartbeats with the ResourceManager in milliseconds if Flink requests containers:")
					.list(
						text("The lower this value is, the faster Flink will get notified about container allocations since requests and allocations are transmitted via heartbeats."),
						text("The lower this value is, the more excessive containers might get allocated which will eventually be released but put pressure on Yarn."))
					.text("If you observe too many container allocations on the ResourceManager, then it is recommended to increase this value. See %s for more information.", link("https://issues.apache.org/jira/browse/YARN-1902", "this link"))
					.build());

	/**
	 * When a Flink job is submitted to YARN, the JobManager's host and the number of available
	 * processing slots is written into a properties file, so that the Flink client is able
	 * to pick those details up.
	 * This configuration parameter allows changing the default location of that file (for example
	 * for environments sharing a Flink installation between users)
	 */
	public static final ConfigOption<String> PROPERTIES_FILE_LOCATION =
		key("yarn.properties-file.location")
		.noDefaultValue()
		.withDescription("When a Flink job is submitted to YARN, the JobManager’s host and the number of available" +
			" processing slots is written into a properties file, so that the Flink client is able to pick those" +
			" details up. This configuration parameter allows changing the default location of that file" +
			" (for example for environments sharing a Flink installation between users).");

	/**
	 * The config parameter defining the Akka actor system port for the ApplicationMaster and
	 * JobManager.
	 * The port can either be a port, such as "9123",
	 * a range of ports: "50100-50200"
	 * or a list of ranges and or points: "50100-50200,50300-50400,51234".
	 * Setting the port to 0 will let the OS choose an available port.
	 */
	public static final ConfigOption<String> APPLICATION_MASTER_PORT =
		key("yarn.application-master.port")
		.defaultValue("0")
		.withDescription("With this configuration option, users can specify a port, a range of ports or a list of ports" +
			" for the Application Master (and JobManager) RPC port. By default we recommend using the default value (0)" +
			" to let the operating system choose an appropriate port. In particular when multiple AMs are running on" +
			" the same physical host, fixed port assignments prevent the AM from starting. For example when running" +
			" Flink on YARN on an environment with a restrictive firewall, this option allows specifying a range of" +
			" allowed ports.");

	/**
	 * A comma-separated list of strings to use as YARN application tags.
	 */
	public static final ConfigOption<String> APPLICATION_TAGS =
		key("yarn.tags")
		.defaultValue("")
		.withDescription("A comma-separated list of tags to apply to the Flink YARN application.");

	// ------------------------------------------------------------------------

	/** This class is not meant to be instantiated. */
	private YarnConfigOptions() {}

	/** @see YarnConfigOptions#CLASSPATH_INCLUDE_USER_JAR */
	public enum UserJarInclusion {
		DISABLED,
		FIRST,
		LAST,
		ORDER
	}
}
