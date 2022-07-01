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

package org.apache.flink.connector.pulsar.sink.writer.topic.register;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.connector.pulsar.sink.config.SinkConfiguration;
import org.apache.flink.connector.pulsar.sink.writer.topic.TopicExtractor;
import org.apache.flink.connector.pulsar.sink.writer.topic.TopicExtractor.TopicMetadataProvider;
import org.apache.flink.connector.pulsar.sink.writer.topic.TopicRegister;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicMetadata;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava30.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava30.com.google.common.cache.CacheLoader;
import org.apache.flink.shaded.guava30.com.google.common.cache.LoadingCache;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonList;
import static org.apache.flink.connector.pulsar.common.config.PulsarClientFactory.createAdmin;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils.topicNameWithPartition;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** The register for returning dynamic topic partitions information. */
@Internal
public class DynamicTopicRegister<IN> implements TopicRegister<IN> {
    private static final long serialVersionUID = 4374769306761301456L;

    private final TopicExtractor<IN> topicExtractor;

    // Dynamic fields.
    private transient PulsarAdmin pulsarAdmin;
    private transient TopicMetadataProvider metadataProvider;
    private transient LoadingCache<String, List<String>> partitionsCache;

    public DynamicTopicRegister(TopicExtractor<IN> topicExtractor) {
        this.topicExtractor = checkNotNull(topicExtractor);
    }

    @Override
    public List<String> topics(IN in) {
        TopicPartition partition = topicExtractor.extract(in, metadataProvider);
        String topicName = partition.getFullTopicName();

        if (partition.isPartition()) {
            return singletonList(topicName);
        } else {
            try {
                return partitionsCache.get(topicName);
            } catch (ExecutionException e) {
                throw new FlinkRuntimeException("Failed to query Pulsar topic partitions.", e);
            }
        }
    }

    @Override
    public void open(SinkConfiguration sinkConfiguration, ProcessingTimeService timeService) {
        long refreshInterval = sinkConfiguration.getTopicMetadataRefreshInterval();

        // Initialize Pulsar admin instance.
        this.pulsarAdmin = createAdmin(sinkConfiguration);
        this.metadataProvider = new DefaultTopicMetadataProvider(pulsarAdmin, refreshInterval);
        this.partitionsCache =
                CacheBuilder.newBuilder()
                        .expireAfterWrite(refreshInterval, TimeUnit.MILLISECONDS)
                        .build(
                                new CacheLoader<String, List<String>>() {
                                    @Override
                                    public List<String> load(String topic) throws Exception {
                                        TopicMetadata metadata = metadataProvider.query(topic);
                                        if (metadata.isPartitioned()) {
                                            int partitionSize = metadata.getPartitionSize();
                                            List<String> partitions =
                                                    new ArrayList<>(partitionSize);
                                            for (int i = 0; i < partitionSize; i++) {
                                                partitions.add(topicNameWithPartition(topic, i));
                                            }
                                            return partitions;
                                        } else {
                                            return singletonList(topic);
                                        }
                                    }
                                });
        // Open the topic extractor instance.
        topicExtractor.open(sinkConfiguration);
    }

    @Override
    public void close() throws IOException {
        if (pulsarAdmin != null) {
            pulsarAdmin.close();
        }
    }

    private static class DefaultTopicMetadataProvider implements TopicMetadataProvider {

        private final LoadingCache<String, TopicMetadata> metadataCache;

        private DefaultTopicMetadataProvider(PulsarAdmin pulsarAdmin, long refreshInterval) {
            this.metadataCache =
                    CacheBuilder.newBuilder()
                            .expireAfterWrite(refreshInterval, TimeUnit.MILLISECONDS)
                            .build(
                                    new CacheLoader<String, TopicMetadata>() {
                                        @Override
                                        public TopicMetadata load(String topic) throws Exception {
                                            PartitionedTopicMetadata metadata =
                                                    pulsarAdmin
                                                            .topics()
                                                            .getPartitionedTopicMetadata(topic);
                                            return new TopicMetadata(topic, metadata.partitions);
                                        }
                                    });
        }

        @Override
        public TopicMetadata query(String topic) throws ExecutionException {
            return metadataCache.get(topic);
        }
    }
}
