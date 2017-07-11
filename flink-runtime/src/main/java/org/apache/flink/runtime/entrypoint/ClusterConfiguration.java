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

package org.apache.flink.runtime.entrypoint;

import org.apache.flink.util.Preconditions;

/**
 * Configuration class which contains the parsed command line arguments for
 * the {@link ClusterEntrypoint}.
 */
public class ClusterConfiguration {
	private final String configDir;

	public ClusterConfiguration(String configDir) {
		this.configDir = Preconditions.checkNotNull(configDir);
	}

	public String getConfigDir() {
		return configDir;
	}
}
