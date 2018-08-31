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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.util.Preconditions;

/**
 * Configuration for the {@link JobMaster}.
 */
public class JobMasterConfiguration {

	private final Time rpcTimeout;

	private final Time slotRequestTimeout;

	private final String tmpDirectory;

	private final Configuration configuration;

	public JobMasterConfiguration(
			Time rpcTimeout,
			Time slotRequestTimeout,
			String tmpDirectory,
			Configuration configuration) {
		this.rpcTimeout = Preconditions.checkNotNull(rpcTimeout);
		this.slotRequestTimeout = Preconditions.checkNotNull(slotRequestTimeout);
		this.tmpDirectory = Preconditions.checkNotNull(tmpDirectory);
		this.configuration = Preconditions.checkNotNull(configuration);
	}

	public Time getRpcTimeout() {
		return rpcTimeout;
	}

	public Time getSlotRequestTimeout() {
		return slotRequestTimeout;
	}

	public String getTmpDirectory() {
		return tmpDirectory;
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	public static JobMasterConfiguration fromConfiguration(Configuration configuration) {

		final Time rpcTimeout = AkkaUtils.getTimeoutAsTime(configuration);

		final Time slotRequestTimeout = Time.milliseconds(configuration.getLong(JobManagerOptions.SLOT_REQUEST_TIMEOUT));

		final String tmpDirectory = ConfigurationUtils.parseTempDirectories(configuration)[0];

		return new JobMasterConfiguration(
			rpcTimeout,
			slotRequestTimeout,
			tmpDirectory,
			configuration);
	}
}
