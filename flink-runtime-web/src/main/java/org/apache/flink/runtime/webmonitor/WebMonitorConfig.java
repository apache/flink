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


import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.NetUtils;

import java.util.Iterator;

public class WebMonitorConfig {

	// ------------------------------------------------------------------------
	//  Config Keys
	// ------------------------------------------------------------------------

	/** The port for the runtime monitor web-frontend server. */
	public static final String JOB_MANAGER_WEB_PORT_KEY = ConfigConstants.JOB_MANAGER_WEB_PORT_KEY;

	/** The initial refresh interval for the web dashboard */
	public static final String JOB_MANAGER_WEB_REFRESH_INTERVAL_KEY = "jobmanager.web.refresh-interval";


	// ------------------------------------------------------------------------
	//  Default values
	// ------------------------------------------------------------------------

	/** Default port for the web dashboard (= 8081) */
	public static final String DEFAULT_JOB_MANAGER_WEB_FRONTEND_PORT = ConfigConstants.DEFAULT_JOB_MANAGER_WEB_FRONTEND_PORT;

	/** Default refresh interval for the web dashboard (= 3000 msecs) */
	public static final long DEFAULT_JOB_MANAGER_WEB_REFRESH_INTERVAL = 3000;


	// ------------------------------------------------------------------------
	//  Config
	// ------------------------------------------------------------------------

	/** The configuration queried by this config object */
	private final Configuration config;


	public WebMonitorConfig(Configuration config) {
		if (config == null) {
			throw new NullPointerException();
		}
		this.config = config;
	}

	public String getWebFrontendAddress() {
		return config.getValue(ConfigConstants.DEFAULT_JOB_MANAGER_WEB_FRONTEND_ADDRESS);
	}

	public Iterator<Integer> getWebFrontendPortRange() throws IllegalArgumentException {
		String serverPortRange = config.getString(JOB_MANAGER_WEB_PORT_KEY, DEFAULT_JOB_MANAGER_WEB_FRONTEND_PORT);

		try {
			return NetUtils.getPortRangeFromString(serverPortRange);
		} catch (Exception e) {
			throw new IllegalArgumentException("Invalid port range definition: " + serverPortRange);
		}
	}

	public long getRefreshInterval() {
		return config.getLong(JOB_MANAGER_WEB_REFRESH_INTERVAL_KEY, DEFAULT_JOB_MANAGER_WEB_REFRESH_INTERVAL);
	}
	
	public boolean isProgramSubmitEnabled() {
		return config.getBoolean(
			ConfigConstants.JOB_MANAGER_WEB_SUBMIT_ENABLED_KEY,
			ConfigConstants.DEFAULT_JOB_MANAGER_WEB_SUBMIT_ENABLED);
	}
}
