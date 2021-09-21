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

package org.apache.flink.tests.util.pulsar.cases;

import org.apache.flink.connector.pulsar.testutils.PulsarTestEnvironment;
import org.apache.flink.connector.pulsar.testutils.cases.MultipleTopicTemplateContext;

import org.apache.pulsar.client.api.SubscriptionType;

import static org.apache.flink.connector.pulsar.testutils.runtime.container.PulsarContainerRuntime.PULSAR_ADMIN_URL;
import static org.apache.flink.connector.pulsar.testutils.runtime.container.PulsarContainerRuntime.PULSAR_SERVICE_URL;

/** We would consuming from test splits by using {@link SubscriptionType#Exclusive} subscription. */
public class ExclusiveSubscriptionContext extends MultipleTopicTemplateContext {
    private static final long serialVersionUID = 6238209089442257487L;

    public ExclusiveSubscriptionContext(PulsarTestEnvironment environment) {
        super(environment);
    }

    @Override
    protected String displayName() {
        return "consuming message by Exclusive";
    }

    @Override
    protected String subscriptionName() {
        return "pulsar-exclusive-subscription";
    }

    @Override
    protected SubscriptionType subscriptionType() {
        return SubscriptionType.Exclusive;
    }

    @Override
    protected String serviceUrl() {
        return PULSAR_SERVICE_URL;
    }

    @Override
    protected String adminUrl() {
        return PULSAR_ADMIN_URL;
    }
}
