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

package org.apache.flink.runtime.testutils;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.UnmodifiableConfiguration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.highavailability.nonha.embedded.HaLeadershipControl;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.util.Preconditions;

/** Mini cluster resource configuration object. */
public class MiniClusterResourceConfiguration {

    private final UnmodifiableConfiguration configuration;

    private final int numberTaskManagers;

    private final int numberSlotsPerTaskManager;

    private final Time shutdownTimeout;

    private final RpcServiceSharing rpcServiceSharing;

    private final MiniCluster.HaServices haServices;

    protected MiniClusterResourceConfiguration(
            Configuration configuration,
            int numberTaskManagers,
            int numberSlotsPerTaskManager,
            Time shutdownTimeout,
            RpcServiceSharing rpcServiceSharing,
            MiniCluster.HaServices haServices) {

        this.configuration =
                new UnmodifiableConfiguration(Preconditions.checkNotNull(configuration));
        this.numberTaskManagers = numberTaskManagers;
        this.numberSlotsPerTaskManager = numberSlotsPerTaskManager;
        this.shutdownTimeout = Preconditions.checkNotNull(shutdownTimeout);
        this.rpcServiceSharing = Preconditions.checkNotNull(rpcServiceSharing);
        this.haServices = haServices;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public int getNumberTaskManagers() {
        return numberTaskManagers;
    }

    public int getNumberSlotsPerTaskManager() {
        return numberSlotsPerTaskManager;
    }

    public Time getShutdownTimeout() {
        return shutdownTimeout;
    }

    public RpcServiceSharing getRpcServiceSharing() {
        return rpcServiceSharing;
    }

    public MiniCluster.HaServices getHaServices() {
        return haServices;
    }

    /** Builder for {@link MiniClusterResourceConfiguration}. */
    public static final class Builder {

        private Configuration configuration = new Configuration();
        private int numberTaskManagers = 1;
        private int numberSlotsPerTaskManager = 1;
        private Time shutdownTimeout = AkkaUtils.getTimeoutAsTime(configuration);

        private RpcServiceSharing rpcServiceSharing = RpcServiceSharing.SHARED;
        private MiniCluster.HaServices haServices = MiniCluster.HaServices.CONFIGURED;

        public Builder setConfiguration(Configuration configuration) {
            this.configuration = configuration;
            return this;
        }

        public Builder setNumberTaskManagers(int numberTaskManagers) {
            this.numberTaskManagers = numberTaskManagers;
            return this;
        }

        public Builder setNumberSlotsPerTaskManager(int numberSlotsPerTaskManager) {
            this.numberSlotsPerTaskManager = numberSlotsPerTaskManager;
            return this;
        }

        public Builder setShutdownTimeout(Time shutdownTimeout) {
            this.shutdownTimeout = shutdownTimeout;
            return this;
        }

        public Builder setRpcServiceSharing(RpcServiceSharing rpcServiceSharing) {
            this.rpcServiceSharing = rpcServiceSharing;
            return this;
        }

        /**
         * Enables or disables {@link HaLeadershipControl} in {@link
         * MiniCluster#getHaLeadershipControl}.
         *
         * <p>{@link HaLeadershipControl} allows granting and revoking leadership of HA components.
         * Enabling this feature disables {@link HighAvailabilityOptions#HA_MODE} option.
         */
        public Builder withHaLeadershipControl() {
            this.haServices = MiniCluster.HaServices.WITH_LEADERSHIP_CONTROL;
            return this;
        }

        public MiniClusterResourceConfiguration build() {
            return new MiniClusterResourceConfiguration(
                    configuration,
                    numberTaskManagers,
                    numberSlotsPerTaskManager,
                    shutdownTimeout,
                    rpcServiceSharing,
                    haServices);
        }
    }
}
