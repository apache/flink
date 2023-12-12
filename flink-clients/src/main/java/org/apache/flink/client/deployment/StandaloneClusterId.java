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

package org.apache.flink.client.deployment;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.util.AbstractID;

import java.util.Objects;

/** Identifier for standalone clusters. */
public class StandaloneClusterId {
    private static final String STANDALONE_CLUSTER_ID_PREFIX = "standalone-flink-cluster-";
    private final String clusterId;

    private StandaloneClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    public static StandaloneClusterId fromConfiguration(Configuration configuration) {
        String clusterId;
        if (HighAvailabilityMode.isHighAvailabilityModeActivated(configuration)) {
            clusterId = configuration.get(HighAvailabilityOptions.HA_CLUSTER_ID);
        } else if (!configuration.get(RestOptions.ADDRESS).isEmpty()) {
            final String address = configuration.get(RestOptions.ADDRESS);
            final int port = configuration.getInteger(RestOptions.PORT);
            clusterId = String.format("%s:%s", address, port);
        } else {
            clusterId = generateStandaloneClusterId();
        }

        return new StandaloneClusterId(clusterId);
    }

    private static String generateStandaloneClusterId() {
        final String randomID = new AbstractID().toString();
        return (STANDALONE_CLUSTER_ID_PREFIX + randomID).substring(0, 64);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StandaloneClusterId that = (StandaloneClusterId) o;
        return Objects.equals(clusterId, that.clusterId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterId);
    }

    @Override
    public String toString() {
        return "StandaloneClusterId{" + "clusterId='" + clusterId + '\'' + '}';
    }
}
