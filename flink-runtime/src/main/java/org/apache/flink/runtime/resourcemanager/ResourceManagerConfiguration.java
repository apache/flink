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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.resourcemanager.exceptions.ConfigurationException;
import org.apache.flink.util.Preconditions;
import scala.concurrent.duration.Duration;

/**
 * Resource manager configuration
 */
public class ResourceManagerConfiguration {

	private final Time timeout;
	private final Time heartbeatInterval;

	public ResourceManagerConfiguration(Time timeout, Time heartbeatInterval) {
		this.timeout = Preconditions.checkNotNull(timeout);
		this.heartbeatInterval = Preconditions.checkNotNull(heartbeatInterval);
	}

	public Time getTimeout() {
		return timeout;
	}

	public Time getHeartbeatInterval() {
		return heartbeatInterval;
	}

	// --------------------------------------------------------------------------
	// Static factory methods
	// --------------------------------------------------------------------------

	public static ResourceManagerConfiguration fromConfiguration(Configuration configuration) throws ConfigurationException {
		ConfigOption<String> timeoutOption = ConfigOptions
			.key(ConfigConstants.AKKA_ASK_TIMEOUT)
			.defaultValue(ConfigConstants.DEFAULT_AKKA_ASK_TIMEOUT);

		final String strTimeout = configuration.getString(timeoutOption);
		final Time timeout;

		try {
			timeout = Time.milliseconds(Duration.apply(strTimeout).toMillis());
		} catch (NumberFormatException e) {
			throw new ConfigurationException("Could not parse the resource manager's timeout " +
				"value " + timeoutOption + '.', e);
		}

		ConfigOption<String> heartbeatIntervalOption = ConfigOptions
			.key(ConfigConstants.AKKA_WATCH_HEARTBEAT_INTERVAL)
			.defaultValue(timeout.toString());

		final String strHeartbeatInterval = configuration.getString(heartbeatIntervalOption);
		final Time heartbeatInterval;

		try {
			heartbeatInterval = Time.milliseconds(Duration.apply(strHeartbeatInterval).toMillis());
		} catch (NumberFormatException e) {
			throw new ConfigurationException("Could not parse the resource manager's heartbeat interval " +
				"value " + timeoutOption + '.', e);
		}

		return new ResourceManagerConfiguration(timeout, heartbeatInterval);
	}
}
