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

import org.apache.flink.shaded.guava30.com.google.common.base.Strings;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerInterceptor;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.ConsumerBuilderImpl;
import org.apache.pulsar.client.impl.ConsumerInterceptors;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.common.naming.TopicName;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.pulsar.client.util.RetryMessageUtil.DLQ_GROUP_TOPIC_SUFFIX;
import static org.apache.pulsar.client.util.RetryMessageUtil.MAX_RECONSUMETIMES;
import static org.apache.pulsar.client.util.RetryMessageUtil.RETRY_GROUP_TOPIC_SUFFIX;

/** Override the default consumer builder for supporting build the custom Key_Shared consumer. */
public class PulsarConsumerBuilder<T> extends ConsumerBuilderImpl<T> {

    public PulsarConsumerBuilder(PulsarClient client, Schema<T> schema) {
        super((PulsarClientImpl) client, schema);
    }

    @Override
    public CompletableFuture<Consumer<T>> subscribeAsync() {
        PulsarClientImpl client = super.getClient();
        ConsumerConfigurationData<T> conf = super.getConf();
        Schema<T> schema = super.getSchema();
        List<ConsumerInterceptor<T>> interceptorList = super.getInterceptorList();

        // Override the default subscribeAsync for skipping the subscription validation.
        if (conf.isRetryEnable()) {
            TopicName topicFirst = TopicName.get(conf.getTopicNames().iterator().next());
            String retryLetterTopic =
                    topicFirst + "-" + conf.getSubscriptionName() + RETRY_GROUP_TOPIC_SUFFIX;
            String deadLetterTopic =
                    topicFirst + "-" + conf.getSubscriptionName() + DLQ_GROUP_TOPIC_SUFFIX;

            DeadLetterPolicy deadLetterPolicy = conf.getDeadLetterPolicy();
            if (deadLetterPolicy == null) {
                conf.setDeadLetterPolicy(
                        DeadLetterPolicy.builder()
                                .maxRedeliverCount(MAX_RECONSUMETIMES)
                                .retryLetterTopic(retryLetterTopic)
                                .deadLetterTopic(deadLetterTopic)
                                .build());
            } else {
                if (Strings.isNullOrEmpty(deadLetterPolicy.getRetryLetterTopic())) {
                    deadLetterPolicy.setRetryLetterTopic(retryLetterTopic);
                }
                if (Strings.isNullOrEmpty(deadLetterPolicy.getDeadLetterTopic())) {
                    deadLetterPolicy.setDeadLetterTopic(deadLetterTopic);
                }
            }

            conf.getTopicNames().add(conf.getDeadLetterPolicy().getRetryLetterTopic());
        }

        if (interceptorList == null || interceptorList.isEmpty()) {
            return client.subscribeAsync(conf, schema, null);
        } else {
            return client.subscribeAsync(conf, schema, new ConsumerInterceptors<>(interceptorList));
        }
    }
}
