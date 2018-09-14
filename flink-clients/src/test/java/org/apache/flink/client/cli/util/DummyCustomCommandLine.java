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

package org.apache.flink.client.cli.util;

import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.util.Preconditions;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import javax.annotation.Nullable;

/**
 * Dummy implementation of the {@link CustomCommandLine} for testing purposes.
 */
public class DummyCustomCommandLine<T> implements CustomCommandLine {
	private final ClusterClient<T> clusterClient;

	public DummyCustomCommandLine(ClusterClient<T> clusterClient) {
		this.clusterClient = Preconditions.checkNotNull(clusterClient);
	}

	@Override
	public boolean isActive(CommandLine commandLine) {
		return true;
	}

	@Override
	public String getId() {
		return DummyCustomCommandLine.class.getSimpleName();
	}

	@Override
	public void addRunOptions(Options baseOptions) {
		// nothing to add
	}

	@Override
	public void addGeneralOptions(Options baseOptions) {
		// nothing to add
	}

	@Override
	public ClusterDescriptor<T> createClusterDescriptor(CommandLine commandLine) {
		return new DummyClusterDescriptor<>(clusterClient);
	}

	@Override
	@Nullable
	public String getClusterId(CommandLine commandLine) {
		return "dummy";
	}

	@Override
	public ClusterSpecification getClusterSpecification(CommandLine commandLine) {
		return new ClusterSpecification.ClusterSpecificationBuilder().createClusterSpecification();
	}
}
