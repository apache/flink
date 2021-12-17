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

package org.apache.flink.connector.aws.util;

import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.connector.aws.config.AWSConfigConstants.CredentialProvider;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;
import software.amazon.awssdk.http.Protocol;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.internal.NettyConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.utils.AttributeMap;

import java.nio.file.Paths;
import java.time.Duration;
import java.util.Properties;

import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_CREDENTIALS_PROVIDER;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_REGION;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.CredentialProvider.ASSUME_ROLE;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.CredentialProvider.AUTO;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.CredentialProvider.BASIC;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.CredentialProvider.WEB_IDENTITY_TOKEN;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.roleArn;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.roleSessionName;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.webIdentityTokenFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.awssdk.http.Protocol.HTTP1_1;
import static software.amazon.awssdk.http.Protocol.HTTP2;

/** Tests for {@link AWSGeneralUtil}. */
public class AWSGeneralUtilTest {
    @Rule public ExpectedException exception = ExpectedException.none();

    @Test
    public void testGetCredentialsProviderTypeDefaultsAuto() {
        assertEquals(
                AUTO,
                AWSGeneralUtil.getCredentialProviderType(
                        new Properties(), AWS_CREDENTIALS_PROVIDER));
    }

    @Test
    public void testGetCredentialsProviderTypeBasic() {
        Properties testConfig =
                TestUtil.properties(AWSConfigConstants.accessKeyId(AWS_CREDENTIALS_PROVIDER), "ak");
        testConfig.setProperty(AWSConfigConstants.secretKey(AWS_CREDENTIALS_PROVIDER), "sk");

        assertEquals(
                BASIC,
                AWSGeneralUtil.getCredentialProviderType(testConfig, AWS_CREDENTIALS_PROVIDER));
    }

    @Test
    public void testGetCredentialsProviderTypeWebIdentityToken() {
        Properties testConfig = TestUtil.properties(AWS_CREDENTIALS_PROVIDER, "WEB_IDENTITY_TOKEN");

        CredentialProvider type =
                AWSGeneralUtil.getCredentialProviderType(testConfig, AWS_CREDENTIALS_PROVIDER);
        assertEquals(WEB_IDENTITY_TOKEN, type);
    }

    @Test
    public void testGetCredentialsProviderTypeAssumeRole() {
        Properties testConfig = TestUtil.properties(AWS_CREDENTIALS_PROVIDER, "ASSUME_ROLE");

        CredentialProvider type =
                AWSGeneralUtil.getCredentialProviderType(testConfig, AWS_CREDENTIALS_PROVIDER);
        assertEquals(ASSUME_ROLE, type);
    }

    @Test
    public void testGetCredentialsProviderEnvironmentVariables() {
        Properties properties = TestUtil.properties(AWS_CREDENTIALS_PROVIDER, "ENV_VAR");

        AwsCredentialsProvider credentialsProvider =
                AWSGeneralUtil.getCredentialsProvider(properties);

        assertTrue(credentialsProvider instanceof EnvironmentVariableCredentialsProvider);
    }

    @Test
    public void testGetCredentialsProviderSystemProperties() {
        Properties properties = TestUtil.properties(AWS_CREDENTIALS_PROVIDER, "SYS_PROP");

        AwsCredentialsProvider credentialsProvider =
                AWSGeneralUtil.getCredentialsProvider(properties);

        assertTrue(credentialsProvider instanceof SystemPropertyCredentialsProvider);
    }

    @Test
    public void testGetCredentialsProviderWebIdentityTokenFileCredentialsProvider() {
        Properties properties = TestUtil.properties(AWS_CREDENTIALS_PROVIDER, "WEB_IDENTITY_TOKEN");

        AwsCredentialsProvider credentialsProvider =
                AWSGeneralUtil.getCredentialsProvider(properties);

        assertTrue(credentialsProvider instanceof WebIdentityTokenFileCredentialsProvider);
    }

    @Test
    public void testGetWebIdentityTokenFileCredentialsProvider() {
        Properties properties = TestUtil.properties(AWS_CREDENTIALS_PROVIDER, "WEB_IDENTITY_TOKEN");
        properties.setProperty(roleArn(AWS_CREDENTIALS_PROVIDER), "roleArn");
        properties.setProperty(roleSessionName(AWS_CREDENTIALS_PROVIDER), "roleSessionName");

        WebIdentityTokenFileCredentialsProvider.Builder builder =
                mockWebIdentityTokenFileCredentialsProviderBuilder();

        AWSGeneralUtil.getWebIdentityTokenFileCredentialsProvider(
                builder, properties, AWS_CREDENTIALS_PROVIDER);

        verify(builder).roleArn("roleArn");
        verify(builder).roleSessionName("roleSessionName");
        verify(builder, never()).webIdentityTokenFile(any());
    }

    @Test
    public void testGetWebIdentityTokenFileCredentialsProviderWithWebIdentityFile() {
        Properties properties = TestUtil.properties(AWS_CREDENTIALS_PROVIDER, "WEB_IDENTITY_TOKEN");
        properties.setProperty(
                webIdentityTokenFile(AWS_CREDENTIALS_PROVIDER), "webIdentityTokenFile");

        WebIdentityTokenFileCredentialsProvider.Builder builder =
                mockWebIdentityTokenFileCredentialsProviderBuilder();

        AWSGeneralUtil.getWebIdentityTokenFileCredentialsProvider(
                builder, properties, AWS_CREDENTIALS_PROVIDER);

        verify(builder).webIdentityTokenFile(Paths.get("webIdentityTokenFile"));
    }

    @Test
    public void testGetCredentialsProviderAuto() {
        Properties properties = TestUtil.properties(AWS_CREDENTIALS_PROVIDER, "AUTO");

        AwsCredentialsProvider credentialsProvider =
                AWSGeneralUtil.getCredentialsProvider(properties);

        assertTrue(credentialsProvider instanceof DefaultCredentialsProvider);
    }

    @Test
    public void testGetCredentialsProviderAssumeRole() {
        Properties properties = spy(TestUtil.properties(AWS_CREDENTIALS_PROVIDER, "ASSUME_ROLE"));
        properties.setProperty(AWS_REGION, "eu-west-2");

        AwsCredentialsProvider credentialsProvider =
                AWSGeneralUtil.getCredentialsProvider(properties);

        assertTrue(credentialsProvider instanceof StsAssumeRoleCredentialsProvider);

        verify(properties).getProperty(AWSConfigConstants.roleArn(AWS_CREDENTIALS_PROVIDER));
        verify(properties)
                .getProperty(AWSConfigConstants.roleSessionName(AWS_CREDENTIALS_PROVIDER));
        verify(properties).getProperty(AWSConfigConstants.externalId(AWS_CREDENTIALS_PROVIDER));
        verify(properties).getProperty(AWS_REGION);
    }

    @Test
    public void testGetCredentialsProviderBasic() {
        Properties properties = TestUtil.properties(AWS_CREDENTIALS_PROVIDER, "BASIC");
        properties.setProperty(AWSConfigConstants.accessKeyId(AWS_CREDENTIALS_PROVIDER), "ak");
        properties.setProperty(AWSConfigConstants.secretKey(AWS_CREDENTIALS_PROVIDER), "sk");

        AwsCredentials credentials =
                AWSGeneralUtil.getCredentialsProvider(properties).resolveCredentials();

        assertEquals("ak", credentials.accessKeyId());
        assertEquals("sk", credentials.secretAccessKey());
    }

    @Test
    public void testGetCredentialsProviderProfile() {
        Properties properties = TestUtil.properties(AWS_CREDENTIALS_PROVIDER, "PROFILE");
        properties.put(AWSConfigConstants.profileName(AWS_CREDENTIALS_PROVIDER), "default");
        properties.put(
                AWSConfigConstants.profilePath(AWS_CREDENTIALS_PROVIDER),
                "src/test/resources/profile");

        AwsCredentialsProvider credentialsProvider =
                AWSGeneralUtil.getCredentialsProvider(properties);

        assertTrue(credentialsProvider instanceof ProfileCredentialsProvider);

        AwsCredentials credentials = credentialsProvider.resolveCredentials();
        assertEquals("11111111111111111111", credentials.accessKeyId());
        assertEquals("wJalrXUtnFEMI/K7MDENG/bPxRfiCY1111111111", credentials.secretAccessKey());
    }

    @Test
    public void testGetCredentialsProviderNamedProfile() {
        Properties properties = TestUtil.properties(AWS_CREDENTIALS_PROVIDER, "PROFILE");
        properties.setProperty(AWSConfigConstants.profileName(AWS_CREDENTIALS_PROVIDER), "foo");
        properties.setProperty(
                AWSConfigConstants.profilePath(AWS_CREDENTIALS_PROVIDER),
                "src/test/resources/profile");

        AwsCredentialsProvider credentialsProvider =
                AWSGeneralUtil.getCredentialsProvider(properties);

        assertTrue(credentialsProvider instanceof ProfileCredentialsProvider);

        AwsCredentials credentials = credentialsProvider.resolveCredentials();
        assertEquals("22222222222222222222", credentials.accessKeyId());
        assertEquals("wJalrXUtnFEMI/K7MDENG/bPxRfiCY2222222222", credentials.secretAccessKey());
    }

    @Test
    public void testCreateNettyAsyncHttpClientWithPropertyTcpKeepAlive() throws Exception {
        SdkAsyncHttpClient httpClient = AWSGeneralUtil.createAsyncHttpClient(new Properties());
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        assertTrue(nettyConfiguration.tcpKeepAlive());
    }

    @Test
    public void testCreateNettyAsyncHttpClientWithPropertyMaxConcurrency() throws Exception {
        int maxConnections = 45678;
        Properties properties = new Properties();
        properties.setProperty(
                AWSConfigConstants.HTTP_CLIENT_MAX_CONCURRENCY, String.valueOf(maxConnections));

        SdkAsyncHttpClient httpClient = AWSGeneralUtil.createAsyncHttpClient(properties);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        assertEquals(maxConnections, nettyConfiguration.maxConnections());
    }

    @Test
    public void testCreateNettyAsyncHttpClientWithPropertyReadTimeout() throws Exception {
        int readTimeoutMillis = 45678;
        Properties properties = new Properties();
        properties.setProperty(
                AWSConfigConstants.HTTP_CLIENT_READ_TIMEOUT_MILLIS,
                String.valueOf(readTimeoutMillis));

        SdkAsyncHttpClient httpClient = AWSGeneralUtil.createAsyncHttpClient(properties);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        assertEquals(readTimeoutMillis, nettyConfiguration.readTimeoutMillis());
    }

    @Test
    public void testCreateNettyAsyncHttpClientWithPropertyTrustAllCertificates() throws Exception {
        boolean trustAllCerts = true;
        Properties properties = new Properties();
        properties.setProperty(
                AWSConfigConstants.TRUST_ALL_CERTIFICATES, String.valueOf(trustAllCerts));

        SdkAsyncHttpClient httpClient = AWSGeneralUtil.createAsyncHttpClient(properties);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        assertEquals(trustAllCerts, nettyConfiguration.trustAllCertificates());
    }

    @Test
    public void testCreateNettyAsyncHttpClientWithPropertyProtocol() throws Exception {
        Protocol httpVersion = HTTP1_1;
        Properties properties = new Properties();
        properties.setProperty(
                AWSConfigConstants.HTTP_PROTOCOL_VERSION, String.valueOf(httpVersion));

        SdkAsyncHttpClient httpClient = AWSGeneralUtil.createAsyncHttpClient(properties);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        assertEquals(
                httpVersion, nettyConfiguration.attribute(SdkHttpConfigurationOption.PROTOCOL));
    }

    @Test
    public void testCreateNettyAsyncHttpClientWithDefaultsConnectionAcquireTimeout()
            throws Exception {
        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();

        SdkAsyncHttpClient httpClient = AWSGeneralUtil.createAsyncHttpClient(builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        assertEquals(60_000, nettyConfiguration.connectionAcquireTimeoutMillis());
    }

    @Test
    public void testCreateNettyAsyncHttpClientWithDefaultsConnectionTtl() throws Exception {
        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();

        SdkAsyncHttpClient httpClient = AWSGeneralUtil.createAsyncHttpClient(builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        SdkAsyncHttpClient httpDefaultClient = NettyNioAsyncHttpClient.create();
        NettyConfiguration nettyDefaultConfiguration =
                TestUtil.getNettyConfiguration(httpDefaultClient);

        assertEquals(
                nettyDefaultConfiguration.connectionTtlMillis(),
                nettyConfiguration.connectionTtlMillis());
    }

    @Test
    public void testCreateNettyAsyncHttpClientWithDefaultsConnectionTimeout() throws Exception {
        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();

        SdkAsyncHttpClient httpClient = AWSGeneralUtil.createAsyncHttpClient(builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        SdkAsyncHttpClient httpDefaultClient = NettyNioAsyncHttpClient.create();
        NettyConfiguration nettyDefaultConfiguration =
                TestUtil.getNettyConfiguration(httpDefaultClient);

        assertEquals(
                nettyDefaultConfiguration.connectTimeoutMillis(),
                nettyConfiguration.connectTimeoutMillis());
    }

    @Test
    public void testCreateNettyAsyncHttpClientWithDefaultsIdleTimeout() throws Exception {
        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();

        SdkAsyncHttpClient httpClient = AWSGeneralUtil.createAsyncHttpClient(builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        SdkAsyncHttpClient httpDefaultClient = NettyNioAsyncHttpClient.create();
        NettyConfiguration nettyDefaultConfiguration =
                TestUtil.getNettyConfiguration(httpDefaultClient);

        assertEquals(
                nettyDefaultConfiguration.idleTimeoutMillis(),
                nettyConfiguration.idleTimeoutMillis());
    }

    @Test
    public void testCreateNettyAsyncHttpClientWithDefaultsMaxConnections() throws Exception {
        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();

        SdkAsyncHttpClient httpClient = AWSGeneralUtil.createAsyncHttpClient(builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        assertEquals(10_000, nettyConfiguration.maxConnections());
    }

    @Test
    public void testCreateNettyAsyncHttpClientWithDefaultsMaxPendingConnectionAcquires()
            throws Exception {
        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();

        SdkAsyncHttpClient httpClient = AWSGeneralUtil.createAsyncHttpClient(builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        SdkAsyncHttpClient httpDefaultClient = NettyNioAsyncHttpClient.create();
        NettyConfiguration nettyDefaultConfiguration =
                TestUtil.getNettyConfiguration(httpDefaultClient);

        assertEquals(
                nettyDefaultConfiguration.maxPendingConnectionAcquires(),
                nettyConfiguration.maxPendingConnectionAcquires());
    }

    @Test
    public void testCreateNettyAsyncHttpClientWithDefaultsReadTimeout() throws Exception {
        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();

        SdkAsyncHttpClient httpClient = AWSGeneralUtil.createAsyncHttpClient(builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        assertEquals(360_000, nettyConfiguration.readTimeoutMillis());
    }

    @Test
    public void testCreateNettyAsyncHttpClientWithDefaultsReapIdleConnections() throws Exception {
        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();

        SdkAsyncHttpClient httpClient = AWSGeneralUtil.createAsyncHttpClient(builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        SdkAsyncHttpClient httpDefaultClient = NettyNioAsyncHttpClient.create();
        NettyConfiguration nettyDefaultConfiguration =
                TestUtil.getNettyConfiguration(httpDefaultClient);

        assertEquals(
                nettyDefaultConfiguration.reapIdleConnections(),
                nettyConfiguration.reapIdleConnections());
    }

    @Test
    public void testCreateNettyAsyncHttpClientWithDefaultsTcpKeepAlive() throws Exception {
        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();

        SdkAsyncHttpClient httpClient = AWSGeneralUtil.createAsyncHttpClient(builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        SdkAsyncHttpClient httpDefaultClient = NettyNioAsyncHttpClient.create();
        NettyConfiguration nettyDefaultConfiguration =
                TestUtil.getNettyConfiguration(httpDefaultClient);

        assertEquals(nettyDefaultConfiguration.tcpKeepAlive(), nettyConfiguration.tcpKeepAlive());
    }

    @Test
    public void testCreateNettyAsyncHttpClientWithDefaultsTlsKeyManagersProvider()
            throws Exception {
        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();

        SdkAsyncHttpClient httpClient = AWSGeneralUtil.createAsyncHttpClient(builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        SdkAsyncHttpClient httpDefaultClient = NettyNioAsyncHttpClient.create();
        NettyConfiguration nettyDefaultConfiguration =
                TestUtil.getNettyConfiguration(httpDefaultClient);

        assertEquals(
                nettyDefaultConfiguration.tlsKeyManagersProvider(),
                nettyConfiguration.tlsKeyManagersProvider());
    }

    @Test
    public void testCreateNettyAsyncHttpClientWithDefaultsTlsTrustManagersProvider()
            throws Exception {
        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();

        SdkAsyncHttpClient httpClient = AWSGeneralUtil.createAsyncHttpClient(builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        SdkAsyncHttpClient httpDefaultClient = NettyNioAsyncHttpClient.create();
        NettyConfiguration nettyDefaultConfiguration =
                TestUtil.getNettyConfiguration(httpDefaultClient);

        assertEquals(
                nettyDefaultConfiguration.tlsTrustManagersProvider(),
                nettyConfiguration.tlsTrustManagersProvider());
    }

    @Test
    public void testCreateNettyAsyncHttpClientWithDefaultsTrustAllCertificates() throws Exception {
        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();

        SdkAsyncHttpClient httpClient = AWSGeneralUtil.createAsyncHttpClient(builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        assertEquals(false, nettyConfiguration.trustAllCertificates());
    }

    @Test
    public void testCreateNettyAsyncHttpClientWithDefaultsWriteTimeout() throws Exception {
        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();

        SdkAsyncHttpClient httpClient = AWSGeneralUtil.createAsyncHttpClient(builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        SdkAsyncHttpClient httpDefaultClient = NettyNioAsyncHttpClient.create();
        NettyConfiguration nettyDefaultConfiguration =
                TestUtil.getNettyConfiguration(httpDefaultClient);

        assertEquals(
                nettyDefaultConfiguration.writeTimeoutMillis(),
                nettyConfiguration.writeTimeoutMillis());
    }

    @Test
    public void testCreateNettyAsyncHttpClientWithDefaultsProtocol() throws Exception {
        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();

        SdkAsyncHttpClient httpClient = AWSGeneralUtil.createAsyncHttpClient(builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        assertEquals(HTTP2, nettyConfiguration.attribute(SdkHttpConfigurationOption.PROTOCOL));
    }

    @Test
    public void testCreateNettyAsyncHttpClientReadTimeout() throws Exception {
        Duration readTimeout = Duration.ofMillis(1234);

        AttributeMap clientConfiguration =
                AttributeMap.builder()
                        .put(SdkHttpConfigurationOption.READ_TIMEOUT, readTimeout)
                        .build();

        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();
        SdkAsyncHttpClient httpClient =
                AWSGeneralUtil.createAsyncHttpClient(clientConfiguration, builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        assertEquals(readTimeout.toMillis(), nettyConfiguration.readTimeoutMillis());
    }

    @Test
    public void testCreateNettyAsyncHttpClientTcpKeepAlive() throws Exception {
        boolean tcpKeepAlive = true;

        AttributeMap clientConfiguration =
                AttributeMap.builder()
                        .put(SdkHttpConfigurationOption.TCP_KEEPALIVE, tcpKeepAlive)
                        .build();

        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();
        SdkAsyncHttpClient httpClient =
                AWSGeneralUtil.createAsyncHttpClient(clientConfiguration, builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        assertEquals(tcpKeepAlive, nettyConfiguration.tcpKeepAlive());
    }

    @Test
    public void testCreateNettyAsyncHttpClientConnectionTimeout() throws Exception {
        Duration connectionTimeout = Duration.ofMillis(1000);

        AttributeMap clientConfiguration =
                AttributeMap.builder()
                        .put(SdkHttpConfigurationOption.CONNECTION_TIMEOUT, connectionTimeout)
                        .build();

        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();
        SdkAsyncHttpClient httpClient =
                AWSGeneralUtil.createAsyncHttpClient(clientConfiguration, builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        assertEquals(connectionTimeout.toMillis(), nettyConfiguration.connectTimeoutMillis());
    }

    @Test
    public void testCreateNettyAsyncHttpClientMaxConcurrency() throws Exception {
        int maxConnections = 123;

        AttributeMap clientConfiguration =
                AttributeMap.builder()
                        .put(SdkHttpConfigurationOption.MAX_CONNECTIONS, maxConnections)
                        .build();

        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();
        SdkAsyncHttpClient httpClient =
                AWSGeneralUtil.createAsyncHttpClient(clientConfiguration, builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        assertEquals(maxConnections, nettyConfiguration.maxConnections());
    }

    @Test
    public void testCreateNettyAsyncHttpClientWriteTimeout() throws Exception {
        Duration writeTimeout = Duration.ofMillis(3000);

        AttributeMap clientConfiguration =
                AttributeMap.builder()
                        .put(SdkHttpConfigurationOption.WRITE_TIMEOUT, writeTimeout)
                        .build();

        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();
        SdkAsyncHttpClient httpClient =
                AWSGeneralUtil.createAsyncHttpClient(clientConfiguration, builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        assertEquals(writeTimeout.toMillis(), nettyConfiguration.writeTimeoutMillis());
    }

    @Test
    public void testCreateNettyAsyncHttpClientConnectionMaxIdleTime() throws Exception {
        Duration maxIdleTime = Duration.ofMillis(2000);

        AttributeMap clientConfiguration =
                AttributeMap.builder()
                        .put(SdkHttpConfigurationOption.CONNECTION_MAX_IDLE_TIMEOUT, maxIdleTime)
                        .build();

        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();
        SdkAsyncHttpClient httpClient =
                AWSGeneralUtil.createAsyncHttpClient(clientConfiguration, builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        assertEquals(maxIdleTime.toMillis(), nettyConfiguration.idleTimeoutMillis());
    }

    @Test
    public void testCreateNettyAsyncHttpClientIdleConnectionReaper() throws Exception {
        boolean reapIdleConnections = false;

        AttributeMap clientConfiguration =
                AttributeMap.builder()
                        .put(SdkHttpConfigurationOption.REAP_IDLE_CONNECTIONS, reapIdleConnections)
                        .build();

        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();
        SdkAsyncHttpClient httpClient =
                AWSGeneralUtil.createAsyncHttpClient(clientConfiguration, builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        assertEquals(reapIdleConnections, nettyConfiguration.reapIdleConnections());
    }

    @Test
    public void testCreateNettyAsyncHttpClientIdleConnectionTtl() throws Exception {
        Duration connectionTtl = Duration.ofMillis(5000);

        AttributeMap clientConfiguration =
                AttributeMap.builder()
                        .put(SdkHttpConfigurationOption.CONNECTION_TIME_TO_LIVE, connectionTtl)
                        .build();

        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();
        SdkAsyncHttpClient httpClient =
                AWSGeneralUtil.createAsyncHttpClient(clientConfiguration, builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        assertEquals(connectionTtl.toMillis(), nettyConfiguration.connectionTtlMillis());
    }

    @Test
    public void testCreateNettyAsyncHttpClientTrustAllCertificates() throws Exception {
        boolean trustAllCertificates = true;

        AttributeMap clientConfiguration =
                AttributeMap.builder()
                        .put(
                                SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES,
                                trustAllCertificates)
                        .build();

        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();
        SdkAsyncHttpClient httpClient =
                AWSGeneralUtil.createAsyncHttpClient(clientConfiguration, builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        assertEquals(trustAllCertificates, nettyConfiguration.trustAllCertificates());
    }

    @Test
    public void testCreateNettyAsyncHttpClientHttpVersion() throws Exception {
        Protocol httpVersion = HTTP1_1;

        AttributeMap clientConfiguration =
                AttributeMap.builder()
                        .put(SdkHttpConfigurationOption.PROTOCOL, httpVersion)
                        .build();

        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();
        SdkAsyncHttpClient httpClient =
                AWSGeneralUtil.createAsyncHttpClient(clientConfiguration, builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        assertEquals(
                httpVersion, nettyConfiguration.attribute(SdkHttpConfigurationOption.PROTOCOL));
    }

    @Test
    public void testGetRegion() {
        Region region = AWSGeneralUtil.getRegion(TestUtil.properties(AWS_REGION, "eu-west-2"));

        assertEquals(Region.EU_WEST_2, region);
    }

    @Test
    public void testValidRegion() {
        assertTrue(AWSGeneralUtil.isValidRegion(Region.of("us-east-1")));
    }

    @Test
    public void testInvalidRegion() {
        assertFalse(AWSGeneralUtil.isValidRegion(Region.of("ur-east-1")));
    }

    @Test
    public void testUnrecognizableAwsRegionInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Invalid AWS region");

        Properties testConfig = TestUtil.properties(AWSConfigConstants.AWS_REGION, "wrongRegionId");
        testConfig.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
        testConfig.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");

        AWSGeneralUtil.validateAwsConfiguration(testConfig);
    }

    @Test
    public void testCredentialProviderTypeSetToBasicButNoCredentialSetInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Please set values for AWS Access Key ID ('"
                        + AWSConfigConstants.AWS_ACCESS_KEY_ID
                        + "') "
                        + "and Secret Key ('"
                        + AWSConfigConstants.AWS_SECRET_ACCESS_KEY
                        + "') when using the BASIC AWS credential provider type.");

        Properties testConfig = TestUtil.properties(AWSConfigConstants.AWS_REGION, "us-east-1");
        testConfig.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");

        AWSGeneralUtil.validateAwsConfiguration(testConfig);
    }

    @Test
    public void testUnrecognizableCredentialProviderTypeInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Invalid AWS Credential Provider Type");

        Properties testConfig = TestUtil.getStandardProperties();
        testConfig.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "wrongProviderType");

        AWSGeneralUtil.validateAwsConfiguration(testConfig);
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
