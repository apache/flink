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

package org.apache.flink.runtime.rpc.taskexecutor;

import static org.apache.flink.util.Preconditions.checkNotNull;
import org.apache.flink.configuration.Configuration;

import scala.concurrent.duration.FiniteDuration;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 * {@link TaskExecutor} Configuration
 */
public class TaskExecutorConfiguration implements Serializable {

	private static final long serialVersionUID = 1L;

	private final String[] tmpDirPaths;

	private final long cleanupInterval;

	private final int numberOfSlots;

	private final Configuration configuration;

	private final FiniteDuration timeout;
	private final FiniteDuration maxRegistrationDuration;
	private final FiniteDuration initialRegistrationPause;
	private final FiniteDuration maxRegistrationPause;
	private final FiniteDuration refusedRegistrationPause;

	public TaskExecutorConfiguration(
			String[] tmpDirPaths,
			long cleanupInterval,
			FiniteDuration timeout,
			FiniteDuration maxRegistrationDuration,
			int numberOfSlots,
			Configuration configuration) {

		this (tmpDirPaths,
			cleanupInterval,
			timeout,
			maxRegistrationDuration,
			numberOfSlots,
			configuration,
			new FiniteDuration(500, TimeUnit.MILLISECONDS),
			new FiniteDuration(30, TimeUnit.SECONDS),
			new FiniteDuration(10, TimeUnit.SECONDS));
	}

	public TaskExecutorConfiguration(
			String[] tmpDirPaths,
			long cleanupInterval,
			FiniteDuration timeout,
			FiniteDuration maxRegistrationDuration,
			int numberOfSlots,
			Configuration configuration,
			FiniteDuration initialRegistrationPause,
			FiniteDuration maxRegistrationPause,
			FiniteDuration refusedRegistrationPause) {

		this.tmpDirPaths = checkNotNull(tmpDirPaths);
		this.cleanupInterval = checkNotNull(cleanupInterval);
		this.timeout = checkNotNull(timeout);
		this.maxRegistrationDuration = maxRegistrationDuration;
		this.numberOfSlots = checkNotNull(numberOfSlots);
		this.configuration = checkNotNull(configuration);
		this.initialRegistrationPause = checkNotNull(initialRegistrationPause);
		this.maxRegistrationPause = checkNotNull(maxRegistrationPause);
		this.refusedRegistrationPause =checkNotNull(refusedRegistrationPause);
	}

	// --------------------------------------------------------------------------------------------
	//  Properties
	// --------------------------------------------------------------------------------------------

	public String[] getTmpDirPaths() {
		return tmpDirPaths;
	}

	public long getCleanupInterval() {
		return cleanupInterval;
	}

	public FiniteDuration getTimeout() {
		return timeout;
	}

	public FiniteDuration getMaxRegistrationDuration() {
		return maxRegistrationDuration;
	}

	public int getNumberOfSlots() {
		return numberOfSlots;
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	public FiniteDuration getInitialRegistrationPause() {
		return initialRegistrationPause;
	}

	public FiniteDuration getMaxRegistrationPause() {
		return maxRegistrationPause;
	}

	public FiniteDuration getRefusedRegistrationPause() {
		return refusedRegistrationPause;
	}

}

