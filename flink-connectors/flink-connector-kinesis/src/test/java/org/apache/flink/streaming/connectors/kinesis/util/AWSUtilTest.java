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
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants.CredentialProvider;
import org.apache.flink.streaming.connectors.kinesis.model.StartingPosition;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.auth.WebIdentityTokenCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import static com.amazonaws.services.kinesis.model.ShardIteratorType.AT_TIMESTAMP;
import static org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants.AWS_CREDENTIALS_PROVIDER;
import static org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants.CredentialProvider.ASSUME_ROLE;
import static org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants.CredentialProvider.AUTO;
import static org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants.CredentialProvider.BASIC;
import static org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants.CredentialProvider.WEB_IDENTITY_TOKEN;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.DEFAULT_STREAM_TIMESTAMP_DATE_FORMAT;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP;
import static org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber.SENTINEL_AT_TIMESTAMP_SEQUENCE_NUM;
import static org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber.SENTINEL_LATEST_SEQUENCE_NUM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** Tests for AWSUtil. */
@RunWith(PowerMockRunner.class)
@PrepareForTest(AWSUtil.class)
public class AWSUtilTest {

    @Rule private final ExpectedException exception = ExpectedException.none();

    @Test
    public void testDefaultCredentialsProvider() {
        Properties testConfig = new Properties();

        AWSCredentialsProvider credentialsProvider = AWSUtil.getCredentialsProvider(testConfig);

        assertTrue(credentialsProvider instanceof DefaultAWSCredentialsProviderChain);
    }

    @Test
    public void testGetCredentialsProvider() {
        Properties testConfig = new Properties();
        testConfig.setProperty(AWS_CREDENTIALS_PROVIDER, "WEB_IDENTITY_TOKEN");

        AWSCredentialsProvider credentialsProvider = AWSUtil.getCredentialsProvider(testConfig);
        assertTrue(credentialsProvider instanceof WebIdentityTokenCredentialsProvider);
    }

    @Test
    public void testGetCredentialsProviderTypeDefaultsAuto() {
        assertEquals(
                AUTO,
                AWSUtil.getCredentialProviderType(new Properties(), AWS_CREDENTIALS_PROVIDER));
    }

    @Test
    public void testGetCredentialsProviderTypeBasic() {
        Properties testConfig = new Properties();
        testConfig.setProperty(AWSConfigConstants.accessKeyId(AWS_CREDENTIALS_PROVIDER), "ak");
        testConfig.setProperty(AWSConfigConstants.secretKey(AWS_CREDENTIALS_PROVIDER), "sk");

        assertEquals(
                BASIC, AWSUtil.getCredentialProviderType(testConfig, AWS_CREDENTIALS_PROVIDER));
    }

    @Test
    public void testGetCredentialsProviderTypeWebIdentityToken() {
        Properties testConfig = new Properties();
        testConfig.setProperty(AWS_CREDENTIALS_PROVIDER, "WEB_IDENTITY_TOKEN");

        CredentialProvider type =
                AWSUtil.getCredentialProviderType(testConfig, AWS_CREDENTIALS_PROVIDER);
        assertEquals(WEB_IDENTITY_TOKEN, type);
    }

    @Test
    public void testGetCredentialsProviderTypeAssumeRole() {
        Properties testConfig = new Properties();
        testConfig.setProperty(AWS_CREDENTIALS_PROVIDER, "ASSUME_ROLE");

        CredentialProvider type =
                AWSUtil.getCredentialProviderType(testConfig, AWS_CREDENTIALS_PROVIDER);
        assertEquals(ASSUME_ROLE, type);
    }

    @Test
    public void testGetCredentialsProviderEnvironmentVariables() {
        Properties testConfig = new Properties();
        testConfig.setProperty(AWS_CREDENTIALS_PROVIDER, "ENV_VAR");

        AWSCredentialsProvider credentialsProvider = AWSUtil.getCredentialsProvider(testConfig);

        assertTrue(credentialsProvider instanceof EnvironmentVariableCredentialsProvider);
    }

    @Test
    public void testGetCredentialsProviderSystemProperties() {
        Properties testConfig = new Properties();
        testConfig.setProperty(AWS_CREDENTIALS_PROVIDER, "SYS_PROP");

        AWSCredentialsProvider credentialsProvider = AWSUtil.getCredentialsProvider(testConfig);

        assertTrue(credentialsProvider instanceof SystemPropertiesCredentialsProvider);
    }

    @Test
    public void testGetCredentialsProviderBasic() {
        Properties testConfig = new Properties();
        testConfig.setProperty(AWS_CREDENTIALS_PROVIDER, "BASIC");

        testConfig.setProperty(AWSConfigConstants.accessKeyId(AWS_CREDENTIALS_PROVIDER), "ak");
        testConfig.setProperty(AWSConfigConstants.secretKey(AWS_CREDENTIALS_PROVIDER), "sk");

        AWSCredentials credentials = AWSUtil.getCredentialsProvider(testConfig).getCredentials();

        assertEquals("ak", credentials.getAWSAccessKeyId());
        assertEquals("sk", credentials.getAWSSecretKey());
    }

    @Test
    public void testGetCredentialsProviderAuto() {
        Properties testConfig = new Properties();
        testConfig.setProperty(AWS_CREDENTIALS_PROVIDER, "AUTO");

        AWSCredentialsProvider credentialsProvider = AWSUtil.getCredentialsProvider(testConfig);

        assertTrue(credentialsProvider instanceof DefaultAWSCredentialsProviderChain);
    }

    @Test
    public void testInvalidCredentialsProvider() {
        exception.expect(IllegalArgumentException.class);

        Properties testConfig = new Properties();
        testConfig.setProperty(AWS_CREDENTIALS_PROVIDER, "INVALID_PROVIDER");

        AWSUtil.getCredentialsProvider(testConfig);
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

        assertTrue(credentialsProvider instanceof ProfileCredentialsProvider);

        AWSCredentials credentials = credentialsProvider.getCredentials();
        assertEquals("11111111111111111111", credentials.getAWSAccessKeyId());
        assertEquals("wJalrXUtnFEMI/K7MDENG/bPxRfiCY1111111111", credentials.getAWSSecretKey());
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

        assertTrue(credentialsProvider instanceof ProfileCredentialsProvider);

        AWSCredentials credentials = credentialsProvider.getCredentials();
        assertEquals("22222222222222222222", credentials.getAWSAccessKeyId());
        assertEquals("wJalrXUtnFEMI/K7MDENG/bPxRfiCY2222222222", credentials.getAWSSecretKey());
    }

    @Test
    public void testValidRegion() {
        assertTrue(AWSUtil.isValidRegion("us-east-1"));
        assertTrue(AWSUtil.isValidRegion("us-gov-west-1"));
        assertTrue(AWSUtil.isValidRegion("us-isob-east-1"));
        assertTrue(AWSUtil.isValidRegion("aws-global"));
        assertTrue(AWSUtil.isValidRegion("aws-iso-global"));
        assertTrue(AWSUtil.isValidRegion("aws-iso-b-global"));
    }

    @Test
    public void testInvalidRegion() {
        assertFalse(AWSUtil.isValidRegion("invalid-region"));
    }

    @Test
    public void testGetStartingPositionForLatest() {
        StartingPosition position =
                AWSUtil.getStartingPosition(SENTINEL_LATEST_SEQUENCE_NUM.get(), new Properties());

        assertEquals(AT_TIMESTAMP, position.getShardIteratorType());
        assertNotNull(position.getStartingMarker());
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

        assertEquals(AT_TIMESTAMP, position.getShardIteratorType());
        assertEquals(expectedTimestamp, position.getStartingMarker());
    }
}
