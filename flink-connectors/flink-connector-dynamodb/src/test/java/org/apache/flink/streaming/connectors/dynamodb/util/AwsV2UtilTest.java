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

package org.apache.flink.streaming.connectors.dynamodb.util;

import org.apache.flink.streaming.connectors.dynamodb.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.dynamodb.config.AWSConfigConstants.CredentialProvider;

import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.net.URI;
import java.nio.file.Paths;
import java.util.Properties;

import static org.apache.flink.streaming.connectors.dynamodb.config.AWSConfigConstants.AWS_CREDENTIALS_PROVIDER;
import static org.apache.flink.streaming.connectors.dynamodb.config.AWSConfigConstants.AWS_REGION;
import static org.apache.flink.streaming.connectors.dynamodb.config.AWSConfigConstants.CredentialProvider.ASSUME_ROLE;
import static org.apache.flink.streaming.connectors.dynamodb.config.AWSConfigConstants.CredentialProvider.AUTO;
import static org.apache.flink.streaming.connectors.dynamodb.config.AWSConfigConstants.CredentialProvider.BASIC;
import static org.apache.flink.streaming.connectors.dynamodb.config.AWSConfigConstants.CredentialProvider.WEB_IDENTITY_TOKEN;
import static org.apache.flink.streaming.connectors.dynamodb.config.AWSConfigConstants.roleArn;
import static org.apache.flink.streaming.connectors.dynamodb.config.AWSConfigConstants.roleSessionName;
import static org.apache.flink.streaming.connectors.dynamodb.config.AWSConfigConstants.webIdentityTokenFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link AwsV2Util}. */
public class AwsV2UtilTest {

    @Test
    public void testGetCredentialsProviderTypeDefaultsAuto() {
        assertEquals(
                AUTO,
                AwsV2Util.getCredentialProviderType(new Properties(), AWS_CREDENTIALS_PROVIDER));
    }

    @Test
    public void testGetCredentialsProviderTypeBasic() {
        Properties testConfig = new Properties();
        testConfig.setProperty(AWSConfigConstants.accessKeyId(AWS_CREDENTIALS_PROVIDER), "ak");
        testConfig.setProperty(AWSConfigConstants.secretKey(AWS_CREDENTIALS_PROVIDER), "sk");

        assertEquals(
                BASIC, AwsV2Util.getCredentialProviderType(testConfig, AWS_CREDENTIALS_PROVIDER));
    }

    @Test
    public void testGetCredentialsProviderTypeWebIdentityToken() {
        Properties testConfig = new Properties();
        testConfig.setProperty(AWS_CREDENTIALS_PROVIDER, "WEB_IDENTITY_TOKEN");

        CredentialProvider type =
                AwsV2Util.getCredentialProviderType(testConfig, AWS_CREDENTIALS_PROVIDER);
        assertEquals(WEB_IDENTITY_TOKEN, type);
    }

    @Test
    public void testGetCredentialsProviderTypeAssumeRole() {
        Properties testConfig = new Properties();
        testConfig.setProperty(AWS_CREDENTIALS_PROVIDER, "ASSUME_ROLE");

        CredentialProvider type =
                AwsV2Util.getCredentialProviderType(testConfig, AWS_CREDENTIALS_PROVIDER);
        assertEquals(ASSUME_ROLE, type);
    }

    @Test
    public void testGetCredentialsProviderEnvironmentVariables() {
        Properties properties = properties(AWS_CREDENTIALS_PROVIDER, "ENV_VAR");

        AwsCredentialsProvider credentialsProvider = AwsV2Util.getCredentialsProvider(properties);

        assertTrue(credentialsProvider instanceof EnvironmentVariableCredentialsProvider);
    }

    @Test
    public void testGetCredentialsProviderSystemProperties() {
        Properties properties = properties(AWS_CREDENTIALS_PROVIDER, "SYS_PROP");

        AwsCredentialsProvider credentialsProvider = AwsV2Util.getCredentialsProvider(properties);

        assertTrue(credentialsProvider instanceof SystemPropertyCredentialsProvider);
    }

    @Test
    public void testGetCredentialsProviderWebIdentityTokenFileCredentialsProvider() {
        Properties properties = properties(AWS_CREDENTIALS_PROVIDER, "WEB_IDENTITY_TOKEN");

        AwsCredentialsProvider credentialsProvider = AwsV2Util.getCredentialsProvider(properties);

        assertTrue(credentialsProvider instanceof WebIdentityTokenFileCredentialsProvider);
    }

    @Test
    public void testGetWebIdentityTokenFileCredentialsProvider() {
        Properties properties = properties(AWS_CREDENTIALS_PROVIDER, "WEB_IDENTITY_TOKEN");
        properties.setProperty(roleArn(AWS_CREDENTIALS_PROVIDER), "roleArn");
        properties.setProperty(roleSessionName(AWS_CREDENTIALS_PROVIDER), "roleSessionName");

        WebIdentityTokenFileCredentialsProvider.Builder builder =
                mockWebIdentityTokenFileCredentialsProviderBuilder();

        AwsV2Util.getWebIdentityTokenFileCredentialsProvider(
                builder, properties, AWS_CREDENTIALS_PROVIDER);

        verify(builder).roleArn("roleArn");
        verify(builder).roleSessionName("roleSessionName");
        verify(builder, never()).webIdentityTokenFile(any());
    }

    @Test
    public void testGetWebIdentityTokenFileCredentialsProviderWithWebIdentityFile() {
        Properties properties = properties(AWS_CREDENTIALS_PROVIDER, "WEB_IDENTITY_TOKEN");
        properties.setProperty(
                webIdentityTokenFile(AWS_CREDENTIALS_PROVIDER), "webIdentityTokenFile");

        WebIdentityTokenFileCredentialsProvider.Builder builder =
                mockWebIdentityTokenFileCredentialsProviderBuilder();

        AwsV2Util.getWebIdentityTokenFileCredentialsProvider(
                builder, properties, AWS_CREDENTIALS_PROVIDER);

        verify(builder).webIdentityTokenFile(Paths.get("webIdentityTokenFile"));
    }

    @Test
    public void testGetCredentialsProviderAuto() {
        Properties properties = properties(AWS_CREDENTIALS_PROVIDER, "AUTO");

        AwsCredentialsProvider credentialsProvider = AwsV2Util.getCredentialsProvider(properties);

        assertTrue(credentialsProvider instanceof DefaultCredentialsProvider);
    }

    @Test
    public void testGetCredentialsProviderAssumeRole() {
        Properties properties = spy(properties(AWS_CREDENTIALS_PROVIDER, "ASSUME_ROLE"));
        properties.setProperty(AWS_REGION, "eu-west-2");

        AwsCredentialsProvider credentialsProvider = AwsV2Util.getCredentialsProvider(properties);

        assertTrue(credentialsProvider instanceof StsAssumeRoleCredentialsProvider);

        verify(properties).getProperty(AWSConfigConstants.roleArn(AWS_CREDENTIALS_PROVIDER));
        verify(properties)
                .getProperty(AWSConfigConstants.roleSessionName(AWS_CREDENTIALS_PROVIDER));
        verify(properties).getProperty(AWSConfigConstants.externalId(AWS_CREDENTIALS_PROVIDER));
        verify(properties).getProperty(AWS_REGION);
    }

    @Test
    public void testGetCredentialsProviderBasic() {
        Properties properties = properties(AWS_CREDENTIALS_PROVIDER, "BASIC");
        properties.setProperty(AWSConfigConstants.accessKeyId(AWS_CREDENTIALS_PROVIDER), "ak");
        properties.setProperty(AWSConfigConstants.secretKey(AWS_CREDENTIALS_PROVIDER), "sk");

        AwsCredentials credentials =
                AwsV2Util.getCredentialsProvider(properties).resolveCredentials();

        assertEquals("ak", credentials.accessKeyId());
        assertEquals("sk", credentials.secretAccessKey());
    }

    @Test
    public void testGetCredentialsProviderProfile() {
        Properties properties = properties(AWS_CREDENTIALS_PROVIDER, "PROFILE");
        properties.put(AWSConfigConstants.profileName(AWS_CREDENTIALS_PROVIDER), "default");
        properties.put(
                AWSConfigConstants.profilePath(AWS_CREDENTIALS_PROVIDER),
                "src/test/resources/profile");

        AwsCredentialsProvider credentialsProvider = AwsV2Util.getCredentialsProvider(properties);

        assertTrue(credentialsProvider instanceof ProfileCredentialsProvider);

        AwsCredentials credentials = credentialsProvider.resolveCredentials();
        assertEquals("11111111111111111111", credentials.accessKeyId());
        assertEquals("wJalrXUtnFEMI/K7MDENG/bPxRfiCY1111111111", credentials.secretAccessKey());
    }

    @Test
    public void testGetCredentialsProviderNamedProfile() {
        Properties properties = properties(AWS_CREDENTIALS_PROVIDER, "PROFILE");
        properties.setProperty(AWSConfigConstants.profileName(AWS_CREDENTIALS_PROVIDER), "foo");
        properties.setProperty(
                AWSConfigConstants.profilePath(AWS_CREDENTIALS_PROVIDER),
                "src/test/resources/profile");

        AwsCredentialsProvider credentialsProvider = AwsV2Util.getCredentialsProvider(properties);

        assertTrue(credentialsProvider instanceof ProfileCredentialsProvider);

        AwsCredentials credentials = credentialsProvider.resolveCredentials();
        assertEquals("22222222222222222222", credentials.accessKeyId());
        assertEquals("wJalrXUtnFEMI/K7MDENG/bPxRfiCY2222222222", credentials.secretAccessKey());
    }

    @Test
    public void testGetRegion() {
        Region region = AwsV2Util.getRegion(properties(AWS_REGION, "eu-west-2"));

        assertEquals(Region.EU_WEST_2, region);
    }

    @Test
    public void testCreateDynamoDBClient() {
        Properties properties = properties(AWS_REGION, "eu-west-2");
        DynamoDbClientBuilder builder = mockDynamoDbClientBuilder();
        ClientOverrideConfiguration clientOverrideConfiguration =
                ClientOverrideConfiguration.builder().build();

        AwsV2Util.createDynamoDbClient(properties, builder, clientOverrideConfiguration);

        verify(builder).overrideConfiguration(clientOverrideConfiguration);
        verify(builder).region(Region.of("eu-west-2"));
        verify(builder)
                .credentialsProvider(argThat(cp -> cp instanceof DefaultCredentialsProvider));
        verify(builder, never()).endpointOverride(any());
    }

    @Test
    public void testCreateDynamoDBClientWithEndpointOverride() {
        Properties properties = properties(AWS_REGION, "eu-west-2");
        properties.setProperty(AWSConfigConstants.AWS_ENDPOINT, "https://localhost");

        DynamoDbClientBuilder builder = mockDynamoDbClientBuilder();
        ClientOverrideConfiguration clientOverrideConfiguration =
                ClientOverrideConfiguration.builder().build();
        SdkAsyncHttpClient httpClient = NettyNioAsyncHttpClient.builder().build();

        AwsV2Util.createDynamoDbClient(properties, builder, clientOverrideConfiguration);

        verify(builder).endpointOverride(URI.create("https://localhost"));
    }

    @Test
    public void testClientOverrideConfigurationWithDefaults() {
        ClientOverrideConfiguration.Builder builder = mockClientOverrideConfigurationBuilder();

        AwsV2Util.createClientOverrideConfiguration(builder);

        verify(builder).build();
        verify(builder)
                .putAdvancedOption(
                        SdkAdvancedClientOption.USER_AGENT_PREFIX,
                        AwsV2Util.formatFlinkUserAgentPrefix());
        verify(builder, never()).apiCallAttemptTimeout(any());
        verify(builder, never()).apiCallTimeout(any());
    }

    private Properties properties(final String key, final String value) {
        Properties properties = new Properties();
        properties.setProperty(key, value);
        return properties;
    }

    private DynamoDbClientBuilder mockDynamoDbClientBuilder() {
        DynamoDbClientBuilder builder = mock(DynamoDbClientBuilder.class);
        when(builder.overrideConfiguration(any(ClientOverrideConfiguration.class)))
                .thenReturn(builder);
        when(builder.credentialsProvider(any())).thenReturn(builder);
        when(builder.region(any())).thenReturn(builder);

        return builder;
    }

    private ClientOverrideConfiguration.Builder mockClientOverrideConfigurationBuilder() {
        ClientOverrideConfiguration.Builder builder =
                mock(ClientOverrideConfiguration.Builder.class);
        when(builder.putAdvancedOption(any(), any())).thenReturn(builder);
        when(builder.apiCallAttemptTimeout(any())).thenReturn(builder);
        when(builder.apiCallTimeout(any())).thenReturn(builder);

        return builder;
    }

    private WebIdentityTokenFileCredentialsProvider.Builder
            mockWebIdentityTokenFileCredentialsProviderBuilder() {
        WebIdentityTokenFileCredentialsProvider.Builder builder =
                mock(WebIdentityTokenFileCredentialsProvider.Builder.class);
        when(builder.roleArn(any())).thenReturn(builder);
        when(builder.roleSessionName(any())).thenReturn(builder);
        when(builder.webIdentityTokenFile(any())).thenReturn(builder);

        return builder;
    }
}
