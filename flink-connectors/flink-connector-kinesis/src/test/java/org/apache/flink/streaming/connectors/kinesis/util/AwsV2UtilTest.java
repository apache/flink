/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kinesis.util;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
import software.amazon.awssdk.utils.AttributeMap;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFORegistrationType.EAGER;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFORegistrationType.LAZY;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFORegistrationType.NONE;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFO_HTTP_CLIENT_MAX_CONCURRENCY;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFO_HTTP_CLIENT_READ_TIMEOUT_MILLIS;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFO_REGISTRATION_TYPE;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.RECORD_PUBLISHER_TYPE;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.RecordPublisherType.EFO;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.RecordPublisherType.POLLING;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AwsV2Util}. */
public class AwsV2UtilTest {
    @Test
    public void testConvertMaxConcurrency() {
        Integer maxConcurrency = 5;

        Properties properties = new Properties();
        properties.setProperty(EFO_HTTP_CLIENT_MAX_CONCURRENCY, maxConcurrency.toString());

        AttributeMap convertedProperties = AwsV2Util.convertProperties(properties);

        assertThat(convertedProperties.get(SdkHttpConfigurationOption.MAX_CONNECTIONS))
                .isEqualTo(maxConcurrency);
    }

    @Test
    public void testConvertReadTimeout() {
        Duration readTimeout = Duration.ofMillis(1234);

        Properties properties = new Properties();
        properties.setProperty(
                EFO_HTTP_CLIENT_READ_TIMEOUT_MILLIS, String.valueOf(readTimeout.toMillis()));

        AttributeMap convertedProperties = AwsV2Util.convertProperties(properties);

        assertThat(convertedProperties.get(SdkHttpConfigurationOption.READ_TIMEOUT))
                .isEqualTo(readTimeout);
    }

    @Test
    public void testConvertEmpty() {
        Properties properties = new Properties();

        AttributeMap convertedProperties = AwsV2Util.convertProperties(properties);

        assertThat(convertedProperties).isEqualTo(AttributeMap.empty());
    }

    @Test
    public void testIsUsingEfoRecordPublisher() {
        Properties prop = new Properties();
        assertThat(AwsV2Util.isUsingEfoRecordPublisher(prop)).isFalse();

        prop.setProperty(RECORD_PUBLISHER_TYPE, EFO.name());
        assertThat(AwsV2Util.isUsingEfoRecordPublisher(prop)).isTrue();

        prop.setProperty(RECORD_PUBLISHER_TYPE, POLLING.name());
        assertThat(AwsV2Util.isUsingEfoRecordPublisher(prop)).isFalse();
    }

    @Test
    public void testIsEagerEfoRegistrationType() {
        Properties prop = new Properties();
        assertThat(AwsV2Util.isEagerEfoRegistrationType(prop)).isFalse();

        prop.setProperty(EFO_REGISTRATION_TYPE, EAGER.name());
        assertThat(AwsV2Util.isEagerEfoRegistrationType(prop)).isTrue();

        prop.setProperty(EFO_REGISTRATION_TYPE, LAZY.name());
        assertThat(AwsV2Util.isEagerEfoRegistrationType(prop)).isFalse();

        prop.setProperty(EFO_REGISTRATION_TYPE, NONE.name());
        assertThat(AwsV2Util.isEagerEfoRegistrationType(prop)).isFalse();
    }

    @Test
    public void testIsLazyEfoRegistrationType() {
        Properties prop = new Properties();
        assertThat(AwsV2Util.isLazyEfoRegistrationType(prop)).isTrue();

        prop.setProperty(EFO_REGISTRATION_TYPE, EAGER.name());
        assertThat(AwsV2Util.isLazyEfoRegistrationType(prop)).isFalse();

        prop.setProperty(EFO_REGISTRATION_TYPE, LAZY.name());
        assertThat(AwsV2Util.isLazyEfoRegistrationType(prop)).isTrue();

        prop.setProperty(EFO_REGISTRATION_TYPE, NONE.name());
        assertThat(AwsV2Util.isLazyEfoRegistrationType(prop)).isFalse();
    }

    @Test
    public void testIsNoneEfoRegistrationType() {
        Properties prop = new Properties();
        assertThat(AwsV2Util.isNoneEfoRegistrationType(prop)).isFalse();

        prop.setProperty(EFO_REGISTRATION_TYPE, EAGER.name());
        assertThat(AwsV2Util.isNoneEfoRegistrationType(prop)).isFalse();

        prop.setProperty(EFO_REGISTRATION_TYPE, LAZY.name());
        assertThat(AwsV2Util.isNoneEfoRegistrationType(prop)).isFalse();

        prop.setProperty(EFO_REGISTRATION_TYPE, NONE.name());
        assertThat(AwsV2Util.isNoneEfoRegistrationType(prop)).isTrue();
    }

    @Test
    public void testIsRecoverableExceptionForRecoverable() {
        Exception recoverable = LimitExceededException.builder().build();
        assertThat(AwsV2Util.isRecoverableException(new ExecutionException(recoverable))).isTrue();
    }

    @Test
    public void testIsRecoverableExceptionForNonRecoverable() {
        Exception nonRecoverable = new IllegalArgumentException("abc");
        assertThat(AwsV2Util.isRecoverableException(new ExecutionException(nonRecoverable)))
                .isFalse();
    }

    @Test
    public void testIsRecoverableExceptionForRuntimeExceptionWrappingRecoverable() {
        Exception recoverable = LimitExceededException.builder().build();
        Exception runtime = new RuntimeException("abc", recoverable);
        assertThat(AwsV2Util.isRecoverableException(runtime)).isTrue();
    }

    @Test
    public void testIsRecoverableExceptionForRuntimeExceptionWrappingNonRecoverable() {
        Exception nonRecoverable = new IllegalArgumentException("abc");
        Exception runtime = new RuntimeException("abc", nonRecoverable);
        assertThat(AwsV2Util.isRecoverableException(runtime)).isFalse();
    }

    @Test
    public void testIsRecoverableExceptionForNullCause() {
        Exception nonRecoverable = new IllegalArgumentException("abc");
        assertThat(AwsV2Util.isRecoverableException(nonRecoverable)).isFalse();
    }
}
