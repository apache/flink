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

package org.apache.flink.connector.pulsar.source.config;

import org.apache.flink.configuration.Configuration;

import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;

import java.io.Serializable;

import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SUBSCRIPTION_MODE;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SUBSCRIPTION_NAME;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SUBSCRIPTION_TYPE;

/** The configure class for pulsar source. */
public class PulsarSourceConfiguration implements Serializable {
    private static final long serialVersionUID = 8488507275800787580L;

    private final long partitionDiscoveryIntervalMs;

    private final String subscriptionName;

    private final SubscriptionType subscriptionType;

    private final SubscriptionMode subscriptionMode;

    public PulsarSourceConfiguration(Configuration configuration) {
        this.partitionDiscoveryIntervalMs = configuration.get(PARTITION_DISCOVERY_INTERVAL_MS);
        this.subscriptionName = configuration.get(PULSAR_SUBSCRIPTION_NAME);
        this.subscriptionType = configuration.get(PULSAR_SUBSCRIPTION_TYPE);
        this.subscriptionMode = configuration.get(PULSAR_SUBSCRIPTION_MODE);
    }

    public boolean enablePartitionDiscovery() {
        return partitionDiscoveryIntervalMs > 0;
    }

    public long getPartitionDiscoveryIntervalMs() {
        return partitionDiscoveryIntervalMs;
    }

    public String getSubscriptionName() {
        return subscriptionName;
    }

    public SubscriptionType getSubscriptionType() {
        return subscriptionType;
    }

    public SubscriptionMode getSubscriptionMode() {
        return subscriptionMode;
    }

    /** Convert the subscription into a readable str. */
    public String getSubscriptionDesc() {
        return getSubscriptionName()
                + "("
                + getSubscriptionType()
                + ","
                + getSubscriptionMode()
                + ")";
    }
}
