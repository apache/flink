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

import org.apache.flink.annotation.Internal;

import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.utils.AttributeMap;

import java.time.Duration;
import java.util.Properties;

import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFORegistrationType.EAGER;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFORegistrationType.NONE;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFO_HTTP_CLIENT_MAX_CONCURRENCY;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFO_HTTP_CLIENT_READ_TIMEOUT_MILLIS;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFO_REGISTRATION_TYPE;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.RECORD_PUBLISHER_TYPE;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.RecordPublisherType.EFO;

/** Utility methods specific to Amazon Web Service SDK v2.x. */
@Internal
public class AwsV2Util {
    public static AttributeMap convertProperties(Properties properties) {
        AttributeMap.Builder mapBuilder = AttributeMap.builder();
        properties.forEach(
                (k, v) -> {
                    if (k.equals(EFO_HTTP_CLIENT_MAX_CONCURRENCY)) {
                        mapBuilder.put(
                                SdkHttpConfigurationOption.MAX_CONNECTIONS,
                                Integer.parseInt(v.toString()));
                    }

                    if (k.equals(EFO_HTTP_CLIENT_READ_TIMEOUT_MILLIS)) {
                        mapBuilder.put(
                                SdkHttpConfigurationOption.READ_TIMEOUT,
                                Duration.ofMillis(Long.parseLong(v.toString())));
                    }
                });
        return mapBuilder.build();
    }

    public static boolean isUsingEfoRecordPublisher(final Properties properties) {
        return EFO.name().equals(properties.get(RECORD_PUBLISHER_TYPE));
    }

    public static boolean isEagerEfoRegistrationType(final Properties properties) {
        return EAGER.name().equals(properties.get(EFO_REGISTRATION_TYPE));
    }

    public static boolean isLazyEfoRegistrationType(final Properties properties) {
        return !isEagerEfoRegistrationType(properties) && !isNoneEfoRegistrationType(properties);
    }

    public static boolean isNoneEfoRegistrationType(final Properties properties) {
        return NONE.name().equals(properties.get(EFO_REGISTRATION_TYPE));
    }
}
