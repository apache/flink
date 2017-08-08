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
			.defaultValue(0);

	/**
	 * The maximum number of failed Mesos tasks before entirely stopping
	 * the Mesos session / job on Mesos.
	 *
	 * <p>By default, we take the number of initially requested tasks.
	 */
	public static final ConfigOption<Integer> MAX_FAILED_TASKS =
		key("mesos.maximum-failed-tasks")
			.defaultValue(-1);

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
			.noDefaultValue();

	/**
	 * The failover timeout for the Mesos scheduler, after which running tasks are automatically shut down.
	 */
	public static final ConfigOption<Integer> FAILOVER_TIMEOUT_SECONDS =
		key("mesos.failover-timeout")
			.defaultValue(600);

	/**
	 * The config parameter defining the Mesos artifact server port to use.
	 * Setting the port to 0 will let the OS choose an available port.
	 */
	public static final ConfigOption<Integer> ARTIFACT_SERVER_PORT =
		key("mesos.resourcemanager.artifactserver.port")
			.defaultValue(0);

	public static final ConfigOption<String> RESOURCEMANAGER_FRAMEWORK_NAME =
		key("mesos.resourcemanager.framework.name")
			.defaultValue("Flink");

	public static final ConfigOption<String> RESOURCEMANAGER_FRAMEWORK_ROLE =
		key("mesos.resourcemanager.framework.role")
			.defaultValue("*");

	public static final ConfigOption<String> RESOURCEMANAGER_FRAMEWORK_PRINCIPAL =
		key("mesos.resourcemanager.framework.principal")
			.noDefaultValue();

	public static final ConfigOption<String> RESOURCEMANAGER_FRAMEWORK_SECRET =
		key("mesos.resourcemanager.framework.secret")
			.noDefaultValue();

	public static final ConfigOption<String> RESOURCEMANAGER_FRAMEWORK_USER =
		key("mesos.resourcemanager.framework.user")
			.defaultValue("");

	/**
	 * Config parameter to override SSL support for the Artifact Server.
	 */
	public static final ConfigOption<Boolean> ARTIFACT_SERVER_SSL_ENABLED =
		key("mesos.resourcemanager.artifactserver.ssl.enabled")
			.defaultValue(true);

}
