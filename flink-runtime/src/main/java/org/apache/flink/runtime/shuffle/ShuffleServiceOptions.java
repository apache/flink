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

package org.apache.flink.runtime.shuffle;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** Options to configure shuffle service. */
@PublicEvolving
@SuppressWarnings("WeakerAccess")
public class ShuffleServiceOptions {

    public static final String NETTY_SHUFFLE_SERVICE_FACTORY_CLASS =
            "org.apache.flink.runtime.io.network.NettyShuffleServiceFactory";

    private ShuffleServiceOptions() {}

    /**
     * The full class name of the shuffle service factory implementation to be used by the cluster.
     */
    public static final ConfigOption<String> SHUFFLE_SERVICE_FACTORY_CLASS =
            ConfigOptions.key("shuffle-service-factory.class")
                    .stringType()
                    .defaultValue(NETTY_SHUFFLE_SERVICE_FACTORY_CLASS)
                    .withDescription(
                            "The full class name of the shuffle service factory implementation to be used by the cluster. "
                                    + "The default implementation uses Netty for network communication and local memory as well disk space "
                                    + "to store results on a TaskExecutor.");
}
