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

package org.apache.flink.runtime.highavailability;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.runtime.util.ZooKeeperUtils;

public class HighAvailabilityServicesUtils {

	public static HighAvailabilityServices createAvailableOrEmbeddedServices(Configuration config) throws Exception {
		HighAvailabilityMode highAvailabilityMode = LeaderRetrievalUtils.getRecoveryMode(config);

		switch (highAvailabilityMode) {
			case NONE:
				return new EmbeddedNonHaServices();

			case ZOOKEEPER:
				return new ZookeeperHaServices(ZooKeeperUtils.startCuratorFramework(config), 
						Executors.directExecutor(), config);

			default:
				throw new Exception("High availability mode " + highAvailabilityMode + " is not supported.");
		}
	}
	
	
	public static HighAvailabilityServices createHighAvailabilityServices(Configuration configuration) throws Exception {
		HighAvailabilityMode highAvailabilityMode = LeaderRetrievalUtils.getRecoveryMode(configuration);

		switch(highAvailabilityMode) {
			case NONE:
				final String resourceManagerAddress = null;
				return new NonHaServices(resourceManagerAddress);
			case ZOOKEEPER:
				return new ZookeeperHaServices(ZooKeeperUtils.startCuratorFramework(configuration), 
						Executors.directExecutor(), configuration);
			default:
				throw new Exception("Recovery mode " + highAvailabilityMode + " is not supported.");
		}
	}
}
