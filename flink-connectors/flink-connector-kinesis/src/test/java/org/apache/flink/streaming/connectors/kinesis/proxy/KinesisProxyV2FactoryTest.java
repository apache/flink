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

package org.apache.flink.streaming.connectors.kinesis.proxy;

import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.testutils.TestUtils;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.internal.NettyConfiguration;

import java.lang.reflect.Field;
import java.util.Properties;

import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.DEFAULT_EFO_HTTP_CLIENT_READ_TIMEOUT;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFO_HTTP_CLIENT_READ_TIMEOUT_MILLIS;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for methods in the {@link KinesisProxyV2Factory} class. */
public class KinesisProxyV2FactoryTest {

    @Test
    public void testReadTimeoutPopulatedFromDefaults() throws Exception {
        Properties properties = properties();

        KinesisProxyV2Interface proxy = KinesisProxyV2Factory.createKinesisProxyV2(properties);
        NettyConfiguration nettyConfiguration = getNettyConfiguration(proxy);

        assertThat(nettyConfiguration.readTimeoutMillis())
                .isEqualTo(DEFAULT_EFO_HTTP_CLIENT_READ_TIMEOUT.toMillis());
    }

    @Test
    public void testReadTimeoutPopulatedFromProperties() throws Exception {
        Properties properties = properties();
        properties.setProperty(EFO_HTTP_CLIENT_READ_TIMEOUT_MILLIS, "12345");

        KinesisProxyV2Interface proxy = KinesisProxyV2Factory.createKinesisProxyV2(properties);
        NettyConfiguration nettyConfiguration = getNettyConfiguration(proxy);

        assertThat(nettyConfiguration.readTimeoutMillis()).isEqualTo(12345);
    }

    @Test
    public void testClientConfigurationPopulatedTcpKeepAliveDefaults() throws Exception {
        Properties properties = properties();

        KinesisProxyV2Interface proxy = KinesisProxyV2Factory.createKinesisProxyV2(properties);
        NettyConfiguration nettyConfiguration = getNettyConfiguration(proxy);

        assertThat(nettyConfiguration.tcpKeepAlive()).isTrue();
    }

    private NettyConfiguration getNettyConfiguration(final KinesisProxyV2Interface kinesis)
            throws Exception {
        NettyNioAsyncHttpClient httpClient = getField("httpClient", kinesis);
        return getField("configuration", httpClient);
    }

    private <T> T getField(String fieldName, Object obj) throws Exception {
        Field field = obj.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return (T) field.get(obj);
    }

    private Properties properties() {
        Properties properties = TestUtils.efoProperties();
        properties.setProperty(AWSConfigConstants.AWS_REGION, "eu-west-2");
        return properties;
    }
}
