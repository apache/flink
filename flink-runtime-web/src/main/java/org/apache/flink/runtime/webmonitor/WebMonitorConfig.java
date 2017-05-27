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

package org.apache.flink.runtime.webmonitor;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;

/**
 * Configuration object for {@link WebMonitor}.
 */
public class WebMonitorConfig {

	/** The configuration queried by this config object. */
	private final Configuration config;

	public WebMonitorConfig(Configuration config) {
		if (config == null) {
			throw new NullPointerException();
		}
		this.config = config;
	}

	public String getWebFrontendAddress() {
		return config.getValue(JobManagerOptions.WEB_FRONTEND_ADDRESS);
	}

	public int getWebFrontendPort() {
		return config.getInteger(JobManagerOptions.WEB_PORT);
	}

	public long getRefreshInterval() {
		return config.getLong(JobManagerOptions.WEB_REFRESH_INTERVAL);
	}

	public boolean isProgramSubmitEnabled() {
		return config.getBoolean(JobManagerOptions.WEB_SUBMIT_ENABLE);
	}

	public String getAllowOrigin() {
		return config.getString(JobManagerOptions.WEB_ACCESS_CONTROL_ALLOW_ORIGIN);
	}
}
