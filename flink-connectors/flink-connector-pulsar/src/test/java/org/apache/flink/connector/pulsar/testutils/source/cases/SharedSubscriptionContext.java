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

package org.apache.flink.connector.pulsar.testutils.source.cases;

import org.apache.flink.connector.pulsar.testutils.PulsarTestEnvironment;

import org.apache.pulsar.client.api.SubscriptionType;

/** We would consume from test splits by using {@link SubscriptionType#Shared} subscription. */
public class SharedSubscriptionContext extends MultipleTopicConsumingContext {

    public SharedSubscriptionContext(PulsarTestEnvironment environment) {
        super(environment);
    }

    @Override
    protected String displayName() {
        return "consume message by Shared";
    }

    @Override
    protected String subscriptionName() {
        return "pulsar-shared-subscription";
    }

    @Override
    protected SubscriptionType subscriptionType() {
        return SubscriptionType.Shared;
    }
}
