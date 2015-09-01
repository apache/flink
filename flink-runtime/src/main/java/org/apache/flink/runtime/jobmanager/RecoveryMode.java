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

package org.apache.flink.runtime.jobmanager;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;

/**
 * Recovery mode for Flink's cluster execution. Currently supported modes are:
 *
 *   - Standalone: No recovery from JobManager failures
 *   - ZooKeeper: JobManager high availability via ZooKeeper
 *     ZooKeeper is used to select a leader among a group of JobManager. This JobManager
 *     is responsible for the job execution. Upon failure of the leader a new leader is elected
 *     which will take over the responsibilities of the old leader
 */
public enum RecoveryMode {
	STANDALONE,
	ZOOKEEPER;

	/**
	 * Returns true if the defined recovery mode supports high availability.
	 *
	 * @param configuration Configuration which contains the recovery mode
	 * @return true if high availability is supported by the recovery mode, otherwise false
	 */
	public static boolean isHighAvailabilityModeActivated(Configuration configuration) {
		String recoveryMode = configuration.getString(
			ConfigConstants.RECOVERY_MODE,
			ConfigConstants.DEFAULT_RECOVERY_MODE).toUpperCase();

		RecoveryMode mode = RecoveryMode.valueOf(recoveryMode);

		switch(mode) {
			case STANDALONE:
				return false;
			case ZOOKEEPER:
				return true;
			default:
				return false;
		}
	}
}
