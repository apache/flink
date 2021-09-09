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

package org.apache.flink.connector.kafka.source.testutils.hybrid;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.connector.kafka.source.testutils.KafkaSingleTopicExternalContext;
import org.apache.flink.connectors.test.common.external.ExternalContext;
import org.apache.flink.connectors.test.common.external.SourceSplitDataWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;

import java.nio.file.Path;
import java.util.Collection;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class HybridKafkaAndFileExternalContext implements ExternalContext<String> {

    private static final Logger LOG =
            LoggerFactory.getLogger(HybridKafkaAndFileExternalContext.class);

    private final ExternalContext<String> kafkaContext;
    private final ExternalContext<String> fileContext;

    public HybridKafkaAndFileExternalContext(
            ExternalContext<String> kafkaContext, ExternalContext<String> fileContext) {
        this.kafkaContext = kafkaContext;
        this.fileContext = fileContext;
    }

    @Override
    public Source<String, ?, ?> createSource(Boundedness boundedness) {
        Source<String, ?, ?> fileSource = fileContext.createSource(Boundedness.BOUNDED);
        Source<String, ?, ?> kafkaSource = kafkaContext.createSource(boundedness);
        return HybridSource.builder(fileSource).addSource(kafkaSource).build();
    }

    @Override
    public SourceSplitDataWriter<String> createSourceSplitDataWriter(String destination) {
        checkNotNull(destination);
        Destination destEnum = Destination.valueOf(destination);
        switch (destEnum) {
            case KAFKA:
                return kafkaContext.createSourceSplitDataWriter("");
            case FILE:
                return fileContext.createSourceSplitDataWriter("");
            default:
                throw new IllegalStateException("Unexpected value: " + destination);
        }
    }

    @Override
    public Collection<String> generateTestData(int splitIndex, long seed) {
        return fileContext.generateTestData(splitIndex, seed);
    }

    @Override
    public void close() throws Exception {
        kafkaContext.close();
        fileContext.close();
    }

    @Override
    public String toString() {
        return "Hybrid Kafka+File context";
    }

    public enum Destination {
        KAFKA,
        FILE;
    }

    public static class Factory implements ExternalContext.Factory<String> {

        private final KafkaContainer kafkaContainer;
        private final Path fileSourceDir;

        public Factory(KafkaContainer kafkaContainer, Path fileSourceDir) {
            this.kafkaContainer = kafkaContainer;
            this.fileSourceDir = fileSourceDir;
        }

        @Override
        public ExternalContext<String> createExternalContext() {
            return new HybridKafkaAndFileExternalContext(
                    new KafkaSingleTopicExternalContext(getBootstrapServer()),
                    new FileNonSplittableExternalContext(fileSourceDir));
        }

        private String getBootstrapServer() {
            final String internalEndpoints =
                    kafkaContainer.getNetworkAliases().stream()
                            .map(host -> String.join(":", host, Integer.toString(9092)))
                            .collect(Collectors.joining(","));
            return String.join(",", kafkaContainer.getBootstrapServers(), internalEndpoints);
        }
    }
}
