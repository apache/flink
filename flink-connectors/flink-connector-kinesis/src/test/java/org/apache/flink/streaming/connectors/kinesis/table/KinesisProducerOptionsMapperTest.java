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

package org.apache.flink.streaming.connectors.kinesis.table;

import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.table.KinesisConnectorOptionsUtils.KinesisProducerOptionsMapper;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/** Unit tests for {@link KinesisProducerOptionsMapper}. */
public class KinesisProducerOptionsMapperTest {

    @Test
    public void testProducerOptionsMapper() {
        KinesisProducerOptionsMapper producerOptionsMapper =
                new KinesisConnectorOptionsUtils.KinesisProducerOptionsMapper(
                        getDefaultProducerConfigurations());
        Map<String, String> actualMappedProperties =
                producerOptionsMapper.getProcessedResolvedOptions();
        Map<String, String> expectedProperties = getMappedProducerConfigurations();
        Assertions.assertThat(actualMappedProperties).isEqualTo(expectedProperties);
    }

    @Test
    public void testProducerPropertiesExtraction() {
        KinesisProducerOptionsMapper producerOptionsMapper =
                new KinesisConnectorOptionsUtils.KinesisProducerOptionsMapper(
                        getDefaultProducerConfigurations());
        Properties actualMappedProperties = producerOptionsMapper.getValidatedConfigurations();
        Properties expectedProperties = getMappedProducerProperties();
        Assertions.assertThat(actualMappedProperties).isEqualTo(expectedProperties);
    }

    @Test
    public void testProducerEndpointExtraction() {
        Map<String, String> defaultOptions = getDefaultProducerConfigurations();
        Map<String, String> expectedProperties = getMappedProducerConfigurations();
        defaultOptions.put("sink.producer.kinesis-endpoint", "some-end-point.kinesis");
        expectedProperties.put(AWSConfigConstants.AWS_ENDPOINT, "https://some-end-point.kinesis");

        KinesisProducerOptionsMapper producerOptionsMapper =
                new KinesisProducerOptionsMapper(defaultOptions);
        Map<String, String> actualMappedProperties =
                producerOptionsMapper.getProcessedResolvedOptions();

        Assertions.assertThat(actualMappedProperties).isEqualTo(expectedProperties);
    }

    @Test
    public void testProducerEndpointAndPortExtraction() {
        Map<String, String> defaultOptions = getDefaultProducerConfigurations();
        Map<String, String> expectedProperties = getMappedProducerConfigurations();
        defaultOptions.put("sink.producer.kinesis-endpoint", "some-end-point.kinesis");
        defaultOptions.put("sink.producer.kinesis-port", "1234");
        expectedProperties.put(
                AWSConfigConstants.AWS_ENDPOINT, "https://some-end-point.kinesis:1234");

        KinesisProducerOptionsMapper producerOptionsMapper =
                new KinesisProducerOptionsMapper(defaultOptions);
        Map<String, String> actualMappedProperties =
                producerOptionsMapper.getProcessedResolvedOptions();

        Assertions.assertThat(actualMappedProperties).isEqualTo(expectedProperties);
    }

    @Test
    public void testProducerAggregationExtraction() {
        Map<String, String> defaultOptions = getDefaultProducerConfigurations();
        Map<String, String> expectedProperties = getMappedProducerConfigurations();
        defaultOptions.remove("sink.producer.collection-max-size");
        defaultOptions.put("sink.producer.aggregation-enabled", "false");
        expectedProperties.put("sink.batch.max-size", "1");

        KinesisProducerOptionsMapper producerOptionsMapper =
                new KinesisProducerOptionsMapper(defaultOptions);
        Map<String, String> actualMappedProperties =
                producerOptionsMapper.getProcessedResolvedOptions();

        Assertions.assertThat(actualMappedProperties).isEqualTo(expectedProperties);
    }

    @Test
    public void testFailOnInvalidProducerOptions() {
        Map<String, String> defaultOptions = getDefaultProducerConfigurations();
        defaultOptions.put("sink.producer.collection-max-size", "invalid-int");
        KinesisProducerOptionsMapper producerOptionsMapper =
                new KinesisProducerOptionsMapper(defaultOptions);
        Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(producerOptionsMapper::getValidatedConfigurations)
                .withMessageContaining("Invalid option batch size. Must be a positive integer");
    }

    private static Map<String, String> getDefaultProducerConfigurations() {
        Map<String, String> defaultAWSConfigurations = new HashMap<String, String>();
        defaultAWSConfigurations.put("sink.producer.record-max-buffered-time", "1000");
        defaultAWSConfigurations.put("sink.producer.collection-max-size", "100");
        defaultAWSConfigurations.put("sink.producer.collection-max-count", "200");
        defaultAWSConfigurations.put("sink.producer.fail-on-error", "true");
        defaultAWSConfigurations.put("sink.producer.verify-certificate", "false");
        return defaultAWSConfigurations;
    }

    private static Map<String, String> getMappedProducerConfigurations() {
        Map<String, String> defaultExpectedAWSConfigurations = new HashMap<String, String>();
        defaultExpectedAWSConfigurations.put("sink.flush-buffer.timeout", "1000");
        defaultExpectedAWSConfigurations.put("sink.batch.max-size", "100");
        defaultExpectedAWSConfigurations.put("sink.requests.max-inflight", "200");
        defaultExpectedAWSConfigurations.put("sink.fail-on-error", "true");
        defaultExpectedAWSConfigurations.put("aws.trust.all.certificates", "true");
        return defaultExpectedAWSConfigurations;
    }

    private static Properties getMappedProducerProperties() {
        Properties defaultExpectedAWSConfigurations = new Properties();
        defaultExpectedAWSConfigurations.put("sink.flush-buffer.timeout", 1000L);
        defaultExpectedAWSConfigurations.put("sink.batch.max-size", 100);
        defaultExpectedAWSConfigurations.put("sink.requests.max-inflight", 200);
        defaultExpectedAWSConfigurations.put("sink.fail-on-error", true);
        defaultExpectedAWSConfigurations.put("aws.trust.all.certificates", "true");
        return defaultExpectedAWSConfigurations;
    }
}
