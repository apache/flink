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

package org.apache.flink.connector.pulsar.source.enumerator.assigner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.pulsar.source.config.SourceConfiguration;
import org.apache.flink.connector.pulsar.source.enumerator.PulsarSourceEnumState;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;

import org.apache.pulsar.client.api.SubscriptionType;

/** The factory for creating split assigner. */
@Internal
public final class SplitAssignerFactory {

    private SplitAssignerFactory() {
        // No public constructor.
    }

    public static SplitAssigner createAssigner(
            StopCursor stopCursor,
            SourceConfiguration sourceConfiguration,
            SplitEnumeratorContext<PulsarPartitionSplit> context,
            PulsarSourceEnumState enumState) {
        SubscriptionType subscriptionType = sourceConfiguration.getSubscriptionType();
        boolean enablePartitionDiscovery = sourceConfiguration.isEnablePartitionDiscovery();

        switch (subscriptionType) {
            case Failover:
            case Exclusive:
                return new NonSharedSplitAssigner(
                        stopCursor, enablePartitionDiscovery, context, enumState);
            case Shared:
                return new SharedSplitAssigner(
                        stopCursor, enablePartitionDiscovery, context, enumState);
            case Key_Shared:
                return new KeySharedSplitAssigner(
                        stopCursor, enablePartitionDiscovery, context, enumState);
            default:
                throw new IllegalArgumentException(
                        "We don't support this subscription type: " + subscriptionType);
        }
    }
}
