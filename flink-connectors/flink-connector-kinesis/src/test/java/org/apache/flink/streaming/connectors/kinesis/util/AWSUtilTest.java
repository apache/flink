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

import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.model.StartingPosition;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.auth.WebIdentityTokenCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import static com.amazonaws.services.kinesis.model.ShardIteratorType.AT_TIMESTAMP;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_CREDENTIALS_PROVIDER;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.DEFAULT_STREAM_TIMESTAMP_DATE_FORMAT;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP;
import static org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber.SENTINEL_AT_TIMESTAMP_SEQUENCE_NUM;
import static org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber.SENTINEL_LATEST_SEQUENCE_NUM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for AWSUtil. */
@RunWith(PowerMockRunner.class)
@PrepareForTest(AWSUtil.class)
public class AWSUtilTest {

    @Test
    public void testDefaultCredentialsProvider() {
        Properties testConfig = new Properties();

        AWSCredentialsProvider credentialsProvider = AWSUtil.getCredentialsProvider(testConfig);

        assertThat(credentialsProvider).isInstanceOf(DefaultAWSCredentialsProviderChain.class);
    }

    @Test
    public void testGetCredentialsProvider() {
        Properties testConfig = new Properties();
        testConfig.setProperty(AWS_CREDENTIALS_PROVIDER, "WEB_IDENTITY_TOKEN");

        AWSCredentialsProvider credentialsProvider = AWSUtil.getCredentialsProvider(testConfig);
        assertThat(credentialsProvider).isInstanceOf(WebIdentityTokenCredentialsProvider.class);
    }

    @Test
    public void testGetCredentialsProviderEnvironmentVariables() {
        Properties testConfig = new Properties();
        testConfig.setProperty(AWS_CREDENTIALS_PROVIDER, "ENV_VAR");

        AWSCredentialsProvider credentialsProvider = AWSUtil.getCredentialsProvider(testConfig);

        assertThat(credentialsProvider).isInstanceOf(EnvironmentVariableCredentialsProvider.class);
    }

    @Test
    public void testGetCredentialsProviderSystemProperties() {
        Properties testConfig = new Properties();
        testConfig.setProperty(AWS_CREDENTIALS_PROVIDER, "SYS_PROP");

        AWSCredentialsProvider credentialsProvider = AWSUtil.getCredentialsProvider(testConfig);

        assertThat(credentialsProvider).isInstanceOf(SystemPropertiesCredentialsProvider.class);
    }

    @Test
    public void testGetCredentialsProviderBasic() {
        Properties testConfig = new Properties();
        testConfig.setProperty(AWS_CREDENTIALS_PROVIDER, "BASIC");

        testConfig.setProperty(AWSConfigConstants.accessKeyId(AWS_CREDENTIALS_PROVIDER), "ak");
        testConfig.setProperty(AWSConfigConstants.secretKey(AWS_CREDENTIALS_PROVIDER), "sk");

        AWSCredentials credentials = AWSUtil.getCredentialsProvider(testConfig).getCredentials();

        assertThat(credentials.getAWSAccessKeyId()).isEqualTo("ak");
        assertThat(credentials.getAWSSecretKey()).isEqualTo("sk");
    }

    @Test
    public void testGetCredentialsProviderAuto() {
        Properties testConfig = new Properties();
        testConfig.setProperty(AWS_CREDENTIALS_PROVIDER, "AUTO");

        AWSCredentialsProvider credentialsProvider = AWSUtil.getCredentialsProvider(testConfig);

        assertThat(credentialsProvider).isInstanceOf(DefaultAWSCredentialsProviderChain.class);
    }

    @Test
    public void testInvalidCredentialsProvider() {
        assertThatThrownBy(
                        () -> {
                            Properties testConfig = new Properties();
                            testConfig.setProperty(AWS_CREDENTIALS_PROVIDER, "INVALID_PROVIDER");

                            AWSUtil.getCredentialsProvider(testConfig);
                        })
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testGetCredentialsProviderProfile() {
        Properties testConfig = new Properties();
        testConfig.setProperty(AWS_CREDENTIALS_PROVIDER, "PROFILE");
        testConfig.setProperty(AWSConfigConstants.profileName(AWS_CREDENTIALS_PROVIDER), "default");
        testConfig.setProperty(
                AWSConfigConstants.profilePath(AWS_CREDENTIALS_PROVIDER),
                "src/test/resources/profile");

        AWSCredentialsProvider credentialsProvider = AWSUtil.getCredentialsProvider(testConfig);

        assertThat(credentialsProvider).isInstanceOf(ProfileCredentialsProvider.class);

        AWSCredentials credentials = credentialsProvider.getCredentials();
        assertThat(credentials.getAWSAccessKeyId()).isEqualTo("11111111111111111111");
        assertThat(credentials.getAWSSecretKey())
                .isEqualTo("wJalrXUtnFEMI/K7MDENG/bPxRfiCY1111111111");
    }

    @Test
    public void testGetCredentialsProviderNamedProfile() {
        Properties testConfig = new Properties();
        testConfig.setProperty(AWS_CREDENTIALS_PROVIDER, "PROFILE");
        testConfig.setProperty(AWSConfigConstants.profileName(AWS_CREDENTIALS_PROVIDER), "foo");
        testConfig.setProperty(
                AWSConfigConstants.profilePath(AWS_CREDENTIALS_PROVIDER),
                "src/test/resources/profile");

        AWSCredentialsProvider credentialsProvider = AWSUtil.getCredentialsProvider(testConfig);

        assertThat(credentialsProvider).isInstanceOf(ProfileCredentialsProvider.class);

        AWSCredentials credentials = credentialsProvider.getCredentials();
        assertThat(credentials.getAWSAccessKeyId()).isEqualTo("22222222222222222222");
        assertThat(credentials.getAWSSecretKey())
                .isEqualTo("wJalrXUtnFEMI/K7MDENG/bPxRfiCY2222222222");
    }

    @Test
    public void testValidRegion() {
        assertThat(AWSUtil.isValidRegion("us-east-1")).isTrue();
    }

    @Test
    public void testInvalidRegion() {
        assertThat(AWSUtil.isValidRegion("ur-east-1")).isFalse();
    }

    @Test
    public void testGetStartingPositionForLatest() {
        StartingPosition position =
                AWSUtil.getStartingPosition(SENTINEL_LATEST_SEQUENCE_NUM.get(), new Properties());

        assertThat(position.getShardIteratorType()).isEqualTo(AT_TIMESTAMP);
        assertThat(position.getStartingMarker()).isNotNull();
    }

    @Test
    public void testGetStartingPositionForTimestamp() throws Exception {
        String timestamp = "2020-08-13T09:18:00.0+01:00";
        Date expectedTimestamp =
                new SimpleDateFormat(DEFAULT_STREAM_TIMESTAMP_DATE_FORMAT).parse(timestamp);

        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(STREAM_INITIAL_TIMESTAMP, timestamp);

        StartingPosition position =
                AWSUtil.getStartingPosition(
                        SENTINEL_AT_TIMESTAMP_SEQUENCE_NUM.get(), consumerProperties);

        assertThat(position.getShardIteratorType()).isEqualTo(AT_TIMESTAMP);
        assertThat(position.getStartingMarker()).isEqualTo(expectedTimestamp);
    }
}
