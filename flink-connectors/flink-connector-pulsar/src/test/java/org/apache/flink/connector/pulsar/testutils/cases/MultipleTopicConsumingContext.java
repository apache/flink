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

package org.apache.flink.connector.pulsar.testutils.cases;

import org.apache.flink.connector.pulsar.testutils.PulsarTestEnvironment;

import org.apache.pulsar.client.api.SubscriptionType;

import java.net.URL;
import java.util.Collections;
import java.util.List;

/**
 * Pulsar external context that will create multiple topics with only one partitions as source
 * splits.
 */
public class MultipleTopicConsumingContext extends MultipleTopicTemplateContext {

    public MultipleTopicConsumingContext(PulsarTestEnvironment environment) {
        this(environment, Collections.emptyList());
    }

    public MultipleTopicConsumingContext(
            PulsarTestEnvironment environment, List<URL> connectorJarPaths) {
        super(environment, connectorJarPaths);
    }

    @Override
    protected String displayName() {
        return "consuming message on multiple topic";
    }

    @Override
    protected String subscriptionName() {
        return "flink-pulsar-multiple-topic-test";
    }

    @Override
    protected SubscriptionType subscriptionType() {
        return SubscriptionType.Exclusive;
    }
}
