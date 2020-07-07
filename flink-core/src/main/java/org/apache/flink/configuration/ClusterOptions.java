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
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.description.Description;

import static org.apache.flink.configuration.description.TextElement.code;

/**
 * Options which control the cluster behaviour.
 */
@PublicEvolving
public class ClusterOptions {

	@Documentation.Section(Documentation.Sections.EXPERT_FAULT_TOLERANCE)
	public static final ConfigOption<Long> INITIAL_REGISTRATION_TIMEOUT = ConfigOptions
		.key("cluster.registration.initial-timeout")
		.defaultValue(100L)
		.withDescription("Initial registration timeout between cluster components in milliseconds.");

	@Documentation.Section(Documentation.Sections.EXPERT_FAULT_TOLERANCE)
	public static final ConfigOption<Long> MAX_REGISTRATION_TIMEOUT = ConfigOptions
		.key("cluster.registration.max-timeout")
		.defaultValue(30000L)
		.withDescription("Maximum registration timeout between cluster components in milliseconds.");

	@Documentation.Section(Documentation.Sections.EXPERT_FAULT_TOLERANCE)
	public static final ConfigOption<Long> ERROR_REGISTRATION_DELAY = ConfigOptions
		.key("cluster.registration.error-delay")
		.defaultValue(10000L)
		.withDescription("The pause made after an registration attempt caused an exception (other than timeout) in milliseconds.");

	@Documentation.Section(Documentation.Sections.EXPERT_FAULT_TOLERANCE)
	public static final ConfigOption<Long> REFUSED_REGISTRATION_DELAY = ConfigOptions
		.key("cluster.registration.refused-registration-delay")
		.defaultValue(30000L)
		.withDescription("The pause made after the registration attempt was refused in milliseconds.");

	@Documentation.Section(Documentation.Sections.EXPERT_FAULT_TOLERANCE)
	public static final ConfigOption<Long> CLUSTER_SERVICES_SHUTDOWN_TIMEOUT = ConfigOptions
		.key("cluster.services.shutdown-timeout")
		.defaultValue(30000L)
		.withDescription("The shutdown timeout for cluster services like executors in milliseconds.");

	@Documentation.Section(Documentation.Sections.EXPERT_FAULT_TOLERANCE)
	public static final ConfigOption<Integer> CLUSTER_IO_EXECUTOR_POOL_SIZE = ConfigOptions
		.key("cluster.io-pool.size")
		.intType()
		.noDefaultValue()
		.withDescription("The size of the IO executor pool used by the cluster to execute blocking IO operations (Master as well as TaskManager processes). " +
			"By default it will use 4 * the number of CPU cores (hardware contexts) that the cluster process has access to. " +
			"Increasing the pool size allows to run more IO operations concurrently.");

	@Documentation.Section(Documentation.Sections.EXPERT_SCHEDULING)
	public static final ConfigOption<Boolean> EVENLY_SPREAD_OUT_SLOTS_STRATEGY = ConfigOptions
		.key("cluster.evenly-spread-out-slots")
		.defaultValue(false)
		.withDescription(
			Description.builder()
				.text("Enable the slot spread out allocation strategy. This strategy tries to spread out " +
					"the slots evenly across all available %s.", code("TaskExecutors"))
				.build());
}
