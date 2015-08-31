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

package org.apache.flink.runtime.util;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobmanager.RecoveryMode;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderelection.StandaloneLeaderElectionService;

public class LeaderElectionUtils {

	/**
	 * Creates a {@link LeaderElectionService} based on the provided {@link Configuration} object.
	 *
	 * @param configuration Configuration object
	 * @return {@link LeaderElectionService} which was created based on the provided Configuration
	 * @throws Exception
	 */
	public static LeaderElectionService createLeaderElectionService(Configuration configuration) throws Exception {
		RecoveryMode recoveryMode = RecoveryMode.valueOf(configuration.getString(
				ConfigConstants.RECOVERY_MODE,
				ConfigConstants.DEFAULT_RECOVERY_MODE).toUpperCase()
		);

		LeaderElectionService leaderElectionService;

		switch(recoveryMode) {
			case STANDALONE:
				leaderElectionService = new StandaloneLeaderElectionService();
				break;
			case ZOOKEEPER:
				leaderElectionService = ZooKeeperUtils.createLeaderElectionService(configuration);
				break;
			default:
				throw new Exception("Unknown RecoveryMode " + recoveryMode);
		}

		return leaderElectionService;
	}
}
