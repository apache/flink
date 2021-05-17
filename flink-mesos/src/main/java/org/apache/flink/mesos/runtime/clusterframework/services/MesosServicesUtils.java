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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.mesos.configuration.MesosOptions;
import org.apache.flink.mesos.util.MesosArtifactServer;
import org.apache.flink.mesos.util.MesosArtifactServerImpl;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.zookeeper.ZooKeeperUtilityFactory;

import akka.actor.ActorSystem;

import java.util.UUID;

/**
 * Utilities for the {@link MesosServices}.
 *
 * @deprecated Apache Mesos support was deprecated in Flink 1.13 and is subject to removal in the
 *     future (see FLINK-22352 for further details).
 */
@Deprecated
public class MesosServicesUtils {

    /**
     * Creates a {@link MesosServices} instance depending on the high availability settings.
     *
     * @param configuration containing the high availability settings
     * @param hostname the hostname to advertise to remote clients
     * @return a mesos services instance
     * @throws Exception if the mesos services instance could not be created
     */
    public static MesosServices createMesosServices(Configuration configuration, String hostname)
            throws Exception {

        ActorSystem localActorSystem = AkkaUtils.createLocalActorSystem(configuration);

        MesosArtifactServer artifactServer = createArtifactServer(configuration, hostname);

        HighAvailabilityMode highAvailabilityMode = HighAvailabilityMode.fromConfig(configuration);

        switch (highAvailabilityMode) {
            case NONE:
                return new StandaloneMesosServices(localActorSystem, artifactServer);

            case ZOOKEEPER:
                final String zkMesosRootPath =
                        configuration.getString(
                                HighAvailabilityOptions.HA_ZOOKEEPER_MESOS_WORKERS_PATH);

                ZooKeeperUtilityFactory zooKeeperUtilityFactory =
                        new ZooKeeperUtilityFactory(configuration, zkMesosRootPath);

                return new ZooKeeperMesosServices(
                        localActorSystem, artifactServer, zooKeeperUtilityFactory);

            default:
                throw new Exception(
                        "High availability mode " + highAvailabilityMode + " is not supported.");
        }
    }

    private static MesosArtifactServer createArtifactServer(
            Configuration configuration, String hostname) throws Exception {
        final int artifactServerPort =
                configuration.getInteger(MesosOptions.ARTIFACT_SERVER_PORT, 0);

        // a random prefix is affixed to artifact URLs to ensure uniqueness in the Mesos fetcher
        // cache
        final String artifactServerPrefix = UUID.randomUUID().toString();

        return new MesosArtifactServerImpl(
                artifactServerPrefix, hostname, artifactServerPort, configuration);
    }
}
