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

import java.util.List;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * The {@link ConfigOption configuration options} for job execution.
 */
@PublicEvolving
public class PipelineOptions {

	/**
	 * A list of jar files that contain the user-defined function (UDF) classes and all classes used from within the UDFs.
	 */
	public static final ConfigOption<List<String>> JARS =
			key("pipeline.jars")
					.stringType()
					.asList()
					.noDefaultValue()
					.withDescription("A semicolon-separated list of the jars to package with the job jars to be sent to the cluster. These have to be valid paths.");

	/**
	 * A list of URLs that are added to the classpath of each user code classloader of the program.
	 * Paths must specify a protocol (e.g. file://) and be accessible on all nodes
	 */
	public static final ConfigOption<List<String>> CLASSPATHS =
			key("pipeline.classpaths")
					.stringType()
					.asList()
					.noDefaultValue()
					.withDescription("A semicolon-separated list of the classpaths to package with the job jars to be sent to the cluster. These have to be valid URLs.");
}
