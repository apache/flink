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

package org.apache.flink.mesos.configuration;

import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * The set of configuration options relating to mesos settings.
 */
public class MesosOptions {

	/**
	 * The initial number of Mesos tasks to allocate.
	 */
	public static final ConfigOption<Integer> INITIAL_TASKS =
		key("mesos.initial-tasks")
			.defaultValue(0)
			.withDescription("The initial workers to bring up when the master starts");

	/**
	 * The maximum number of failed Mesos tasks before entirely stopping
	 * the Mesos session / job on Mesos.
	 *
	 * <p>By default, we take the number of initially requested tasks.
	 */
	public static final ConfigOption<Integer> MAX_FAILED_TASKS =
		key("mesos.maximum-failed-tasks")
			.defaultValue(-1)
			.withDescription("The maximum number of failed workers before the cluster fails. May be set to -1 to disable" +
				" this feature");

	/**
	 * The Mesos master URL.
	 *
	 * <p>The value should be in one of the following forms:
	 * <pre>
	 * {@code
	 *     host:port
	 *     zk://host1:port1,host2:port2,.../path
	 *     zk://username:password@host1:port1,host2:port2,.../path
	 *     file:///path/to/file (where file contains one of the above)
	 * }
	 * </pre>
	 */
	public static final ConfigOption<String> MASTER_URL =
		key("mesos.master")
			.noDefaultValue()
			.withDescription("The Mesos master URL. The value should be in one of the following forms:" +
				" \"host:port\", \"zk://host1:port1,host2:port2,.../path\"," +
				" \"zk://username:password@host1:port1,host2:port2,.../path\" or \"file:///path/to/file\"");

	/**
	 * The failover timeout for the Mesos scheduler, after which running tasks are automatically shut down.
	 */
	public static final ConfigOption<Integer> FAILOVER_TIMEOUT_SECONDS =
		key("mesos.failover-timeout")
			.defaultValue(60 * 60 * 24 * 7)
			.withDescription("The failover timeout in seconds for the Mesos scheduler, after which running tasks are" +
				" automatically shut down.");

	/**
	 * The config parameter defining the Mesos artifact server port to use.
	 * Setting the port to 0 will let the OS choose an available port.
	 */
	public static final ConfigOption<Integer> ARTIFACT_SERVER_PORT =
		key("mesos.resourcemanager.artifactserver.port")
			.defaultValue(0)
			.withDescription("The config parameter defining the Mesos artifact server port to use. Setting the port to" +
				" 0 will let the OS choose an available port.");

	public static final ConfigOption<String> RESOURCEMANAGER_FRAMEWORK_NAME =
		key("mesos.resourcemanager.framework.name")
			.defaultValue("Flink")
			.withDescription("Mesos framework name");

	public static final ConfigOption<String> RESOURCEMANAGER_FRAMEWORK_ROLE =
		key("mesos.resourcemanager.framework.role")
			.defaultValue("*")
			.withDescription("Mesos framework role definition");

	public static final ConfigOption<String> RESOURCEMANAGER_FRAMEWORK_PRINCIPAL =
		key("mesos.resourcemanager.framework.principal")
			.noDefaultValue()
			.withDescription("Mesos framework principal");

	public static final ConfigOption<String> RESOURCEMANAGER_FRAMEWORK_SECRET =
		key("mesos.resourcemanager.framework.secret")
			.noDefaultValue()
			.withDescription("Mesos framework secret");

	public static final ConfigOption<String> RESOURCEMANAGER_FRAMEWORK_USER =
		key("mesos.resourcemanager.framework.user")
			.defaultValue("")
			.withDescription("Mesos framework user");

	/**
	 * Config parameter to override SSL support for the Artifact Server.
	 */
	public static final ConfigOption<Boolean> ARTIFACT_SERVER_SSL_ENABLED =
		key("mesos.resourcemanager.artifactserver.ssl.enabled")
			.defaultValue(true)
			.withDescription("Enables SSL for the Flink artifact server. Note that security.ssl.enabled also needs to" +
				" be set to true encryption to enable encryption.");

	/**
	 * Config parameter to configure which configuration keys will dynamically get a port assigned through Mesos.
	 */
	public static final ConfigOption<String> PORT_ASSIGNMENTS = key("mesos.resourcemanager.tasks.port-assignments")
		.defaultValue("")
		.withDescription("Comma-separated list of configuration keys which represent a configurable port." +
			"All port keys will dynamically get a port assigned through Mesos.");

}
