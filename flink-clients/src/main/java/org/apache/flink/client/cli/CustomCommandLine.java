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
package org.apache.flink.client.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;


/**
 * Custom command-line interface to load hooks for the command-line interface.
 */
public interface CustomCommandLine<ClusterType extends ClusterClient> {

	/**
	 * Returns a unique identifier for this custom command-line.
	 * @return An unique identifier string
	 */
	String getIdentifier();

	/**
	 * Adds custom options to the existing options.
	 * @param baseOptions The existing options.
	 */
	void addOptions(Options baseOptions);

	/**
	 * Retrieves a client for a running cluster
	 * @param config The Flink config
	 * @return Client if a cluster could be retrieve, null otherwise
	 */
	ClusterClient retrieveCluster(Configuration config) throws Exception;

	/**
	 * Creates the client for the cluster
	 * @param applicationName The application name to use
	 * @param commandLine The command-line options parsed by the CliFrontend
	 * @return The client to communicate with the cluster which the CustomCommandLine brought up.
	 */
	ClusterType createClient(String applicationName, CommandLine commandLine) throws Exception;
}
