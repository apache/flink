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
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import java.net.URL;
import java.util.List;

/**
 * Dummy implementation of the {@link CustomCommandLine} for testing purposes.
 *
 * @param <T> type of the returned cluster client
 */
public class DummyCustomCommandLine<T extends ClusterClient> implements CustomCommandLine<T> {
	private final T clusterClient;

	public DummyCustomCommandLine(T clusterClient) {
		this.clusterClient = Preconditions.checkNotNull(clusterClient);
	}

	@Override
	public boolean isActive(CommandLine commandLine, Configuration configuration) {
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
	public T retrieveCluster(CommandLine commandLine, Configuration config, String configurationDirectory) throws UnsupportedOperationException {
		return clusterClient;
	}

	@Override
	public T createCluster(String applicationName, CommandLine commandLine, Configuration config, String configurationDirectory, List<URL> userJarFiles) throws Exception {
		return clusterClient;
	}
}
