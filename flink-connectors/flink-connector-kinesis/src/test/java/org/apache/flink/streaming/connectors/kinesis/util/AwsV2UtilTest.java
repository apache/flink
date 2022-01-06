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

import org.junit.Test;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.utils.AttributeMap;

import java.time.Duration;
import java.util.Properties;

import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFORegistrationType.EAGER;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFORegistrationType.LAZY;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFORegistrationType.NONE;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFO_HTTP_CLIENT_MAX_CONCURRENCY;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFO_HTTP_CLIENT_READ_TIMEOUT_MILLIS;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFO_REGISTRATION_TYPE;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.RECORD_PUBLISHER_TYPE;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.RecordPublisherType.EFO;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.RecordPublisherType.POLLING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests for {@link AwsV2Util}. */
public class AwsV2UtilTest {
    @Test
    public void testConvertMaxConcurrency() {
        Integer maxConcurrency = 5;

        Properties properties = new Properties();
        properties.setProperty(EFO_HTTP_CLIENT_MAX_CONCURRENCY, maxConcurrency.toString());

        AttributeMap convertedProperties = AwsV2Util.convertProperties(properties);

        assertEquals(
                maxConcurrency,
                convertedProperties.get(SdkHttpConfigurationOption.MAX_CONNECTIONS));
    }

    @Test
    public void testConvertReadTimeout() {
        Duration readTimeout = Duration.ofMillis(1234);

        Properties properties = new Properties();
        properties.setProperty(
                EFO_HTTP_CLIENT_READ_TIMEOUT_MILLIS, String.valueOf(readTimeout.toMillis()));

        AttributeMap convertedProperties = AwsV2Util.convertProperties(properties);

        assertEquals(readTimeout, convertedProperties.get(SdkHttpConfigurationOption.READ_TIMEOUT));
    }

    @Test
    public void testConvertEmpty() {
        Properties properties = new Properties();

        AttributeMap convertedProperties = AwsV2Util.convertProperties(properties);

        assertEquals(AttributeMap.empty(), convertedProperties);
    }

    @Test
    public void testIsUsingEfoRecordPublisher() {
        Properties prop = new Properties();
        assertFalse(AwsV2Util.isUsingEfoRecordPublisher(prop));

        prop.setProperty(RECORD_PUBLISHER_TYPE, EFO.name());
        assertTrue(AwsV2Util.isUsingEfoRecordPublisher(prop));

        prop.setProperty(RECORD_PUBLISHER_TYPE, POLLING.name());
        assertFalse(AwsV2Util.isUsingEfoRecordPublisher(prop));
    }

    @Test
    public void testIsEagerEfoRegistrationType() {
        Properties prop = new Properties();
        assertFalse(AwsV2Util.isEagerEfoRegistrationType(prop));

        prop.setProperty(EFO_REGISTRATION_TYPE, EAGER.name());
        assertTrue(AwsV2Util.isEagerEfoRegistrationType(prop));

        prop.setProperty(EFO_REGISTRATION_TYPE, LAZY.name());
        assertFalse(AwsV2Util.isEagerEfoRegistrationType(prop));

        prop.setProperty(EFO_REGISTRATION_TYPE, NONE.name());
        assertFalse(AwsV2Util.isEagerEfoRegistrationType(prop));
    }

    @Test
    public void testIsLazyEfoRegistrationType() {
        Properties prop = new Properties();
        assertTrue(AwsV2Util.isLazyEfoRegistrationType(prop));

        prop.setProperty(EFO_REGISTRATION_TYPE, EAGER.name());
        assertFalse(AwsV2Util.isLazyEfoRegistrationType(prop));

        prop.setProperty(EFO_REGISTRATION_TYPE, LAZY.name());
        assertTrue(AwsV2Util.isLazyEfoRegistrationType(prop));

        prop.setProperty(EFO_REGISTRATION_TYPE, NONE.name());
        assertFalse(AwsV2Util.isLazyEfoRegistrationType(prop));
    }

    @Test
    public void testIsNoneEfoRegistrationType() {
        Properties prop = new Properties();
        assertFalse(AwsV2Util.isNoneEfoRegistrationType(prop));

        prop.setProperty(EFO_REGISTRATION_TYPE, EAGER.name());
        assertFalse(AwsV2Util.isNoneEfoRegistrationType(prop));

        prop.setProperty(EFO_REGISTRATION_TYPE, LAZY.name());
        assertFalse(AwsV2Util.isNoneEfoRegistrationType(prop));

        prop.setProperty(EFO_REGISTRATION_TYPE, NONE.name());
        assertTrue(AwsV2Util.isNoneEfoRegistrationType(prop));
    }
}
