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

package org.apache.flink.test.util;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;

/**
 * Mirror of {@link org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration} which has
 * been introduced to avoid breaking changes with FLINK-10637.
 *
 * @deprecated This class should be replaced with {@link
 *     org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration}.
 */
@Deprecated
public class MiniClusterResourceConfiguration
        extends org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration {

    MiniClusterResourceConfiguration(
            Configuration configuration,
            int numberTaskManagers,
            int numberSlotsPerTaskManager,
            Time shutdownTimeout,
            RpcServiceSharing rpcServiceSharing) {
        super(
                configuration,
                numberTaskManagers,
                numberSlotsPerTaskManager,
                shutdownTimeout,
                rpcServiceSharing,
                MiniCluster.HaServices.CONFIGURED);
    }

    /** Builder for {@link org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration}. */
    public static final class Builder {

        private Configuration configuration = new Configuration();
        private int numberTaskManagers = 1;
        private int numberSlotsPerTaskManager = 1;
        private Time shutdownTimeout =
                Time.fromDuration(configuration.get(AkkaOptions.ASK_TIMEOUT_DURATION));

        private RpcServiceSharing rpcServiceSharing = RpcServiceSharing.SHARED;

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

        public MiniClusterResourceConfiguration build() {
            return new MiniClusterResourceConfiguration(
                    configuration,
                    numberTaskManagers,
                    numberSlotsPerTaskManager,
                    shutdownTimeout,
                    rpcServiceSharing);
        }
    }
}
