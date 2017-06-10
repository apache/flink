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

package org.apache.flink.mesos.runtime.clusterframework.services;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.zookeeper.ZooKeeperUtilityFactory;
import org.apache.flink.util.ConfigurationUtil;

/**
 * Utilities for the {@link MesosServices}.
 */
public class MesosServicesUtils {

	/**
	 * Creates a {@link MesosServices} instance depending on the high availability settings.
	 *
	 * @param configuration containing the high availability settings
	 * @return a mesos services instance
	 * @throws Exception if the mesos services instance could not be created
	 */
	public static MesosServices createMesosServices(Configuration configuration) throws Exception {
		HighAvailabilityMode highAvailabilityMode = HighAvailabilityMode.fromConfig(configuration);

		switch (highAvailabilityMode) {
			case NONE:
				return new StandaloneMesosServices();

			case ZOOKEEPER:
				final String zkMesosRootPath = ConfigurationUtil.getStringWithDeprecatedKeys(
					configuration,
					ConfigConstants.HA_ZOOKEEPER_MESOS_WORKERS_PATH,
					ConfigConstants.DEFAULT_ZOOKEEPER_MESOS_WORKERS_PATH,
					ConfigConstants.ZOOKEEPER_MESOS_WORKERS_PATH);

				ZooKeeperUtilityFactory zooKeeperUtilityFactory = new ZooKeeperUtilityFactory(
					configuration,
					zkMesosRootPath);

				return new ZooKeeperMesosServices(zooKeeperUtilityFactory);

			default:
				throw new Exception("High availability mode " + highAvailabilityMode + " is not supported.");
		}
	}
}
