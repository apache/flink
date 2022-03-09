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

package org.apache.flink.connector.kinesis.table.util;

import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.connector.kinesis.table.util.KinesisStreamsConnectorOptionsUtils.KinesisProducerOptionsMapper;
import org.apache.flink.util.TestLogger;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/** Unit tests for {@link KinesisProducerOptionsMapper}. */
public class KinesisProducerOptionsMapperTest extends TestLogger {

    @Test
    public void testProducerVerifyCertificateOptionsMapping() {
        Map<String, String> deprecatedOptions = new HashMap<>();
        deprecatedOptions.put("sink.producer.verify-certificate", "false");
        Map<String, String> expectedOptions = new HashMap<>();
        expectedOptions.put(AWSConfigConstants.TRUST_ALL_CERTIFICATES, "true");

        KinesisProducerOptionsMapper producerOptionsMapper =
                new KinesisStreamsConnectorOptionsUtils.KinesisProducerOptionsMapper(
                        deprecatedOptions);
        Map<String, String> actualMappedProperties =
                producerOptionsMapper.mapDeprecatedClientOptions();

        Assertions.assertThat(actualMappedProperties).isEqualTo(expectedOptions);
    }

    @Test
    public void testProducerEndpointExtraction() {
        Map<String, String> deprecatedOptions = new HashMap<>();
        Map<String, String> expectedOptions = new HashMap<>();
        deprecatedOptions.put("sink.producer.kinesis-endpoint", "some-end-point.kinesis");
        expectedOptions.put(AWSConfigConstants.AWS_ENDPOINT, "https://some-end-point.kinesis");

        KinesisProducerOptionsMapper producerOptionsMapper =
                new KinesisProducerOptionsMapper(deprecatedOptions);
        Map<String, String> actualMappedProperties =
                producerOptionsMapper.mapDeprecatedClientOptions();

        Assertions.assertThat(actualMappedProperties).isEqualTo(expectedOptions);
    }

    @Test
    public void testProducerEndpointAndPortExtraction() {
        Map<String, String> deprecatedOptions = new HashMap<>();
        Map<String, String> expectedOptions = new HashMap<>();
        deprecatedOptions.put("sink.producer.kinesis-endpoint", "some-end-point.kinesis");
        deprecatedOptions.put("sink.producer.kinesis-port", "1234");
        expectedOptions.put(AWSConfigConstants.AWS_ENDPOINT, "https://some-end-point.kinesis:1234");

        KinesisProducerOptionsMapper producerOptionsMapper =
                new KinesisProducerOptionsMapper(deprecatedOptions);
        Map<String, String> actualMappedProperties =
                producerOptionsMapper.mapDeprecatedClientOptions();

        Assertions.assertThat(actualMappedProperties).isEqualTo(expectedOptions);
    }
}
