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

import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.util.FlinkException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import javax.annotation.Nullable;

/**
 * Custom command-line interface to load hooks for the command-line interface.
 */
public interface CustomCommandLine<T> {

	/**
	 * Signals whether the custom command-line wants to execute or not.
	 * @param commandLine The command-line options
	 * @return True if the command-line wants to run, False otherwise
	 */
	boolean isActive(CommandLine commandLine);

	/**
	 * Gets the unique identifier of this CustomCommandLine.
	 * @return A unique identifier
	 */
	String getId();

	/**
	 * Adds custom options to the existing run options.
	 * @param baseOptions The existing options.
	 */
	void addRunOptions(Options baseOptions);

	/**
	 * Adds custom options to the existing general options.
	 *
	 * @param baseOptions The existing options.
	 */
	void addGeneralOptions(Options baseOptions);

	/**
	 * Create a {@link ClusterDescriptor} from the given configuration, configuration directory
	 * and the command line.
	 *
	 * @param commandLine containing command line options relevant for the ClusterDescriptor
	 * @return ClusterDescriptor
	 * @throws FlinkException if the ClusterDescriptor could not be created
	 */
	ClusterDescriptor<T> createClusterDescriptor(CommandLine commandLine) throws FlinkException;

	/**
	 * Returns the cluster id if a cluster id was specified on the command line, otherwise it
	 * returns null.
	 *
	 * <p>A cluster id identifies a running cluster, e.g. the Yarn application id for a Flink
	 * cluster running on Yarn.
	 *
	 * @param commandLine containing command line options relevant for the cluster id retrieval
	 * @return Cluster id identifying the cluster to deploy jobs to or null
	 */
	@Nullable
	T getClusterId(CommandLine commandLine);

	/**
	 * Returns the {@link ClusterSpecification} specified by the configuration and the command
	 * line options. This specification can be used to deploy a new Flink cluster.
	 *
	 * @param commandLine containing command line options relevant for the ClusterSpecification
	 * @return ClusterSpecification for a new Flink cluster
	 * @throws FlinkException if the ClusterSpecification could not be created
	 */
	ClusterSpecification getClusterSpecification(CommandLine commandLine) throws FlinkException;

	default CommandLine parseCommandLineOptions(String[] args, boolean stopAtNonOptions) throws CliArgsException {
		final Options options = new Options();
		addGeneralOptions(options);
		addRunOptions(options);
		return CliFrontendParser.parse(options, args, stopAtNonOptions);
	}
}
