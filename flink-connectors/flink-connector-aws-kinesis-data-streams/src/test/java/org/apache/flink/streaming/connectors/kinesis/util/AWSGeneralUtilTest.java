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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Properties;

import static org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants.AWS_CREDENTIALS_PROVIDER;
import static org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants.CredentialProvider.ASSUME_ROLE;
import static org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants.CredentialProvider.AUTO;
import static org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants.CredentialProvider.BASIC;
import static org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants.CredentialProvider.WEB_IDENTITY_TOKEN;
import static org.junit.Assert.assertEquals;

/** Tests for AWSGeneralUtil. */
@RunWith(PowerMockRunner.class)
@PrepareForTest(AWSGeneralUtil.class)
public class AWSGeneralUtilTest {

    @Rule private final ExpectedException exception = ExpectedException.none();

    @Test
    public void testGetCredentialsProviderTypeDefaultsAuto() {
        assertEquals(
                AUTO,
                AWSGeneralUtil.getCredentialProviderType(
                        new Properties(), AWS_CREDENTIALS_PROVIDER));
    }

    @Test
    public void testGetCredentialsProviderTypeBasic() {
        Properties testConfig = new Properties();
        testConfig.setProperty(AWSConfigConstants.accessKeyId(AWS_CREDENTIALS_PROVIDER), "ak");
        testConfig.setProperty(AWSConfigConstants.secretKey(AWS_CREDENTIALS_PROVIDER), "sk");

        assertEquals(
                BASIC,
                AWSGeneralUtil.getCredentialProviderType(testConfig, AWS_CREDENTIALS_PROVIDER));
    }

    @Test
    public void testGetCredentialsProviderTypeWebIdentityToken() {
        Properties testConfig = new Properties();
        testConfig.setProperty(AWS_CREDENTIALS_PROVIDER, "WEB_IDENTITY_TOKEN");

        CredentialProvider type =
                AWSGeneralUtil.getCredentialProviderType(testConfig, AWS_CREDENTIALS_PROVIDER);
        assertEquals(WEB_IDENTITY_TOKEN, type);
    }

    @Test
    public void testGetCredentialsProviderTypeAssumeRole() {
        Properties testConfig = new Properties();
        testConfig.setProperty(AWS_CREDENTIALS_PROVIDER, "ASSUME_ROLE");

        CredentialProvider type =
                AWSGeneralUtil.getCredentialProviderType(testConfig, AWS_CREDENTIALS_PROVIDER);
        assertEquals(ASSUME_ROLE, type);
    }
}
