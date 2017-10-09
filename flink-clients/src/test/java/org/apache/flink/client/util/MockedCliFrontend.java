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

package org.apache.flink.client.util;

import org.apache.flink.client.CliFrontend;
import org.apache.flink.client.CliFrontendTestUtils;
import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;

import org.apache.commons.cli.CommandLine;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Utility class for mocking the {@link ClusterClient} within a {@link CliFrontend}.
 *
 * <p>The mocking behavior can be defined in the constructor of the sub-class.
 */
public class MockedCliFrontend extends CliFrontend {
	public final ClusterClient client;

	protected MockedCliFrontend() throws Exception {
		super(CliFrontendTestUtils.getConfigDir());
		this.client = mock(ClusterClient.class);
	}

	@Override
	public CustomCommandLine getActiveCustomCommandLine(CommandLine commandLine) {
		CustomCommandLine ccl = mock(CustomCommandLine.class);
		when(ccl.retrieveCluster(any(CommandLine.class), any(Configuration.class), anyString()))
			.thenReturn(client);
		return ccl;
	}
}
