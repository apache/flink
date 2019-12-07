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

package org.apache.flink.kubernetes.cli;

import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;

import org.apache.commons.cli.Option;

/**
 * Options for kubernetes cli and entrypoint.
 */
public class KubernetesCliOptions {

	public static Option getOptionWithPrefix(Option option, String shortPrefix, String longPrefix) {
		if (shortPrefix.isEmpty() && longPrefix.isEmpty()) {
			return option;
		}
		return Option.builder(shortPrefix + option.getOpt())
			.longOpt(option.getLongOpt() == null ? null : longPrefix + option.getLongOpt())
			.required(option.isRequired())
			.hasArg(option.hasArg())
			.numberOfArgs(option.getArgs())
			.argName(option.getArgName())
			.desc(option.getDescription())
			.valueSeparator()
			.build();
	}

	public static final Option CLUSTER_ID_OPTION = Option.builder("id")
		.longOpt("clusterId")
		.required(false)
		.hasArg(true)
		.desc(KubernetesConfigOptions.CLUSTER_ID.description().toString())
		.build();

	public static final Option IMAGE_OPTION = Option.builder("i")
		.longOpt("image")
		.required(false)
		.hasArg(true)
		.argName("image-name")
		.desc(KubernetesConfigOptions.CONTAINER_IMAGE.description().toString())
		.build();

	public static final Option JOB_MANAGER_MEMORY_OPTION = Option.builder("jm")
		.longOpt("jobManagerMemory")
		.required(false)
		.hasArg(true)
		.desc("Memory for JobManager Container with optional unit (default: MB)")
		.build();

	public static final Option TASK_MANAGER_MEMORY_OPTION = Option.builder("tm")
		.longOpt("taskManagerMemory")
		.required(false)
		.hasArg(true)
		.desc("Memory per TaskManager Container with optional unit (default: MB)")
		.build();

	public static final Option TASK_MANAGER_SLOTS_OPTION = Option.builder("s")
		.longOpt("slots")
		.required(false)
		.hasArg(true)
		.desc("Number of slots per TaskManager")
		.build();

	public static final Option DYNAMIC_PROPERTY_OPTION = Option.builder("D")
		.argName("property=value")
		.numberOfArgs(2)
		.valueSeparator()
		.desc("use value for given property")
		.build();

	public static final Option HELP_OPTION = Option.builder("h")
		.longOpt("help")
		.hasArg(false)
		.desc("Help for Kubernetes session CLI.")
		.build();

	public static final Option JOB_CLASS_NAME_OPTION = Option.builder("jc")
		.longOpt("job-classname")
		.required(false)
		.hasArg(true)
		.argName("job class name")
		.desc("Class name of the job to run.")
		.build();

	public static final Option JOB_ID_OPTION = Option.builder("jid")
		.longOpt("job-id")
		.required(false)
		.hasArg(true)
		.argName("job id")
		.desc("Job ID of the job to run.")
		.build();

	/** This class is not meant to be instantiated. */
	private KubernetesCliOptions() {}
}
