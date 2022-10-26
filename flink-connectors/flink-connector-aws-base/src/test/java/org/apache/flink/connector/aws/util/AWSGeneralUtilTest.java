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

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.Protocol;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.internal.NettyConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.utils.AttributeMap;
import software.amazon.awssdk.utils.ImmutableMap;

import java.nio.file.Paths;
import java.time.Duration;
import java.util.Map;
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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.awssdk.http.Protocol.HTTP1_1;
import static software.amazon.awssdk.http.Protocol.HTTP2;

/** Tests for {@link AWSGeneralUtil}. */
class AWSGeneralUtilTest {

    @Test
    void testGetCredentialsProviderTypeDefaultsAuto() {
        assertThat(
                        AWSGeneralUtil.getCredentialProviderType(
                                new Properties(), AWS_CREDENTIALS_PROVIDER))
                .isEqualTo(AUTO);
    }

    @Test
    void testGetCredentialsProviderTypeBasic() {
        Properties testConfig =
                TestUtil.properties(AWSConfigConstants.accessKeyId(AWS_CREDENTIALS_PROVIDER), "ak");
        testConfig.setProperty(AWSConfigConstants.secretKey(AWS_CREDENTIALS_PROVIDER), "sk");

        assertThat(AWSGeneralUtil.getCredentialProviderType(testConfig, AWS_CREDENTIALS_PROVIDER))
                .isEqualTo(BASIC);
    }

    @Test
    void testGetCredentialsProviderTypeWebIdentityToken() {
        Properties testConfig = TestUtil.properties(AWS_CREDENTIALS_PROVIDER, "WEB_IDENTITY_TOKEN");

        CredentialProvider type =
                AWSGeneralUtil.getCredentialProviderType(testConfig, AWS_CREDENTIALS_PROVIDER);
        assertThat(type).isEqualTo(WEB_IDENTITY_TOKEN);
    }

    @Test
    void testGetCredentialsProviderTypeAssumeRole() {
        Properties testConfig = TestUtil.properties(AWS_CREDENTIALS_PROVIDER, "ASSUME_ROLE");

        CredentialProvider type =
                AWSGeneralUtil.getCredentialProviderType(testConfig, AWS_CREDENTIALS_PROVIDER);
        assertThat(type).isEqualTo(ASSUME_ROLE);
    }

    @Test
    void testGetCredentialsProviderEnvironmentVariables() {
        Properties properties = TestUtil.properties(AWS_CREDENTIALS_PROVIDER, "ENV_VAR");

        AwsCredentialsProvider credentialsProvider =
                AWSGeneralUtil.getCredentialsProvider(properties);

        assertThat(credentialsProvider).isInstanceOf(EnvironmentVariableCredentialsProvider.class);
    }

    @Test
    void testGetCredentialsProviderSystemProperties() {
        Properties properties = TestUtil.properties(AWS_CREDENTIALS_PROVIDER, "SYS_PROP");

        AwsCredentialsProvider credentialsProvider =
                AWSGeneralUtil.getCredentialsProvider(properties);

        assertThat(credentialsProvider).isInstanceOf(SystemPropertyCredentialsProvider.class);
    }

    @Test
    void testGetCredentialsProviderWebIdentityTokenFileCredentialsProvider() {
        Properties properties = TestUtil.properties(AWS_CREDENTIALS_PROVIDER, "WEB_IDENTITY_TOKEN");

        AwsCredentialsProvider credentialsProvider =
                AWSGeneralUtil.getCredentialsProvider(properties);

        assertThat(credentialsProvider).isInstanceOf(WebIdentityTokenFileCredentialsProvider.class);
    }

    @Test
    void testGetWebIdentityTokenFileCredentialsProvider() {
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
    void testGetWebIdentityTokenFileCredentialsProviderWithWebIdentityFile() {
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
    void testGetCredentialsProviderAuto() {
        Properties properties = TestUtil.properties(AWS_CREDENTIALS_PROVIDER, "AUTO");

        AwsCredentialsProvider credentialsProvider =
                AWSGeneralUtil.getCredentialsProvider(properties);

        assertThat(credentialsProvider).isInstanceOf(DefaultCredentialsProvider.class);
    }

    @Test
    void testGetCredentialsProviderFromMap() {
        Map<String, Object> config = ImmutableMap.of(AWS_CREDENTIALS_PROVIDER, "AUTO");

        AwsCredentialsProvider credentialsProvider = AWSGeneralUtil.getCredentialsProvider(config);

        assertThat(credentialsProvider).isInstanceOf(DefaultCredentialsProvider.class);
    }

    @Test
    void testGetCredentialsProviderAssumeRole() {
        Properties properties = spy(TestUtil.properties(AWS_CREDENTIALS_PROVIDER, "ASSUME_ROLE"));
        properties.setProperty(AWS_REGION, "eu-west-2");

        AwsCredentialsProvider credentialsProvider =
                AWSGeneralUtil.getCredentialsProvider(properties);

        assertThat(credentialsProvider).isInstanceOf(StsAssumeRoleCredentialsProvider.class);

        verify(properties).getProperty(AWSConfigConstants.roleArn(AWS_CREDENTIALS_PROVIDER));
        verify(properties)
                .getProperty(AWSConfigConstants.roleSessionName(AWS_CREDENTIALS_PROVIDER));
        verify(properties).getProperty(AWSConfigConstants.externalId(AWS_CREDENTIALS_PROVIDER));
        verify(properties).getProperty(AWS_REGION);
    }

    @Test
    void testGetCredentialsProviderBasic() {
        Properties properties = TestUtil.properties(AWS_CREDENTIALS_PROVIDER, "BASIC");
        properties.setProperty(AWSConfigConstants.accessKeyId(AWS_CREDENTIALS_PROVIDER), "ak");
        properties.setProperty(AWSConfigConstants.secretKey(AWS_CREDENTIALS_PROVIDER), "sk");

        AwsCredentials credentials =
                AWSGeneralUtil.getCredentialsProvider(properties).resolveCredentials();

        assertThat(credentials.accessKeyId()).isEqualTo("ak");
        assertThat(credentials.secretAccessKey()).isEqualTo("sk");
    }

    @Test
    void testGetCredentialsProviderProfile() {
        Properties properties = TestUtil.properties(AWS_CREDENTIALS_PROVIDER, "PROFILE");
        properties.put(AWSConfigConstants.profileName(AWS_CREDENTIALS_PROVIDER), "default");
        properties.put(
                AWSConfigConstants.profilePath(AWS_CREDENTIALS_PROVIDER),
                "src/test/resources/profile");

        AwsCredentialsProvider credentialsProvider =
                AWSGeneralUtil.getCredentialsProvider(properties);

        assertThat(credentialsProvider).isInstanceOf(ProfileCredentialsProvider.class);

        AwsCredentials credentials = credentialsProvider.resolveCredentials();
        assertThat(credentials.accessKeyId()).isEqualTo("11111111111111111111");
        assertThat(credentials.secretAccessKey())
                .isEqualTo("wJalrXUtnFEMI/K7MDENG/bPxRfiCY1111111111");
    }

    @Test
    void testGetCredentialsProviderNamedProfile() {
        Properties properties = TestUtil.properties(AWS_CREDENTIALS_PROVIDER, "PROFILE");
        properties.setProperty(AWSConfigConstants.profileName(AWS_CREDENTIALS_PROVIDER), "foo");
        properties.setProperty(
                AWSConfigConstants.profilePath(AWS_CREDENTIALS_PROVIDER),
                "src/test/resources/profile");

        AwsCredentialsProvider credentialsProvider =
                AWSGeneralUtil.getCredentialsProvider(properties);

        assertThat(credentialsProvider).isInstanceOf(ProfileCredentialsProvider.class);

        AwsCredentials credentials = credentialsProvider.resolveCredentials();
        assertThat(credentials.accessKeyId()).isEqualTo("22222222222222222222");
        assertThat(credentials.secretAccessKey())
                .isEqualTo("wJalrXUtnFEMI/K7MDENG/bPxRfiCY2222222222");
    }

    @Test
    void testCreateNettyAsyncHttpClientWithPropertyTcpKeepAlive() throws Exception {
        SdkAsyncHttpClient httpClient = AWSGeneralUtil.createAsyncHttpClient(new Properties());
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        assertThat(nettyConfiguration.tcpKeepAlive()).isTrue();
    }

    @Test
    void testCreateNettyAsyncHttpClientWithPropertyMaxConcurrency() throws Exception {
        int maxConnections = 45678;
        Properties properties = new Properties();
        properties.setProperty(
                AWSConfigConstants.HTTP_CLIENT_MAX_CONCURRENCY, String.valueOf(maxConnections));

        SdkAsyncHttpClient httpClient = AWSGeneralUtil.createAsyncHttpClient(properties);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        assertThat(nettyConfiguration.maxConnections()).isEqualTo(maxConnections);
    }

    @Test
    void testCreateNettyAsyncHttpClientWithPropertyReadTimeout() throws Exception {
        int readTimeoutMillis = 45678;
        Properties properties = new Properties();
        properties.setProperty(
                AWSConfigConstants.HTTP_CLIENT_READ_TIMEOUT_MILLIS,
                String.valueOf(readTimeoutMillis));

        SdkAsyncHttpClient httpClient = AWSGeneralUtil.createAsyncHttpClient(properties);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        assertThat(nettyConfiguration.readTimeoutMillis()).isEqualTo(readTimeoutMillis);
    }

    @Test
    void testCreateNettyAsyncHttpClientWithPropertyTrustAllCertificates() throws Exception {
        boolean trustAllCerts = true;
        Properties properties = new Properties();
        properties.setProperty(
                AWSConfigConstants.TRUST_ALL_CERTIFICATES, String.valueOf(trustAllCerts));

        SdkAsyncHttpClient httpClient = AWSGeneralUtil.createAsyncHttpClient(properties);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        assertThat(nettyConfiguration.trustAllCertificates()).isEqualTo(trustAllCerts);
    }

    @Test
    void testCreateNettyAsyncHttpClientWithPropertyProtocol() throws Exception {
        Protocol httpVersion = HTTP1_1;
        Properties properties = new Properties();
        properties.setProperty(
                AWSConfigConstants.HTTP_PROTOCOL_VERSION, String.valueOf(httpVersion));

        SdkAsyncHttpClient httpClient = AWSGeneralUtil.createAsyncHttpClient(properties);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        assertThat(nettyConfiguration.attribute(SdkHttpConfigurationOption.PROTOCOL))
                .isEqualTo(httpVersion);
    }

    @Test
    void testCreateNettyAsyncHttpClientWithDefaultsConnectionAcquireTimeout() throws Exception {
        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();

        SdkAsyncHttpClient httpClient = AWSGeneralUtil.createAsyncHttpClient(builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        assertThat(nettyConfiguration.connectionAcquireTimeoutMillis()).isEqualTo(60_000);
    }

    @Test
    void testCreateNettyAsyncHttpClientWithDefaultsConnectionTtl() throws Exception {
        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();

        SdkAsyncHttpClient httpClient = AWSGeneralUtil.createAsyncHttpClient(builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        SdkAsyncHttpClient httpDefaultClient = NettyNioAsyncHttpClient.create();
        NettyConfiguration nettyDefaultConfiguration =
                TestUtil.getNettyConfiguration(httpDefaultClient);

        assertThat(nettyConfiguration.connectionTtlMillis())
                .isEqualTo(nettyDefaultConfiguration.connectionTtlMillis());
    }

    @Test
    void testCreateNettyAsyncHttpClientWithDefaultsConnectionTimeout() throws Exception {
        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();

        SdkAsyncHttpClient httpClient = AWSGeneralUtil.createAsyncHttpClient(builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        SdkAsyncHttpClient httpDefaultClient = NettyNioAsyncHttpClient.create();
        NettyConfiguration nettyDefaultConfiguration =
                TestUtil.getNettyConfiguration(httpDefaultClient);

        assertThat(nettyDefaultConfiguration.connectTimeoutMillis())
                .isEqualTo(nettyConfiguration.connectTimeoutMillis());
    }

    @Test
    void testCreateNettyAsyncHttpClientWithDefaultsIdleTimeout() throws Exception {
        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();

        SdkAsyncHttpClient httpClient = AWSGeneralUtil.createAsyncHttpClient(builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        SdkAsyncHttpClient httpDefaultClient = NettyNioAsyncHttpClient.create();
        NettyConfiguration nettyDefaultConfiguration =
                TestUtil.getNettyConfiguration(httpDefaultClient);

        assertThat(nettyConfiguration.idleTimeoutMillis())
                .isEqualTo(nettyDefaultConfiguration.idleTimeoutMillis());
    }

    @Test
    void testCreateNettyAsyncHttpClientWithDefaultsMaxConnections() throws Exception {
        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();

        SdkAsyncHttpClient httpClient = AWSGeneralUtil.createAsyncHttpClient(builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        assertThat(nettyConfiguration.maxConnections()).isEqualTo(10_000);
    }

    @Test
    void testCreateNettyAsyncHttpClientWithDefaultsMaxPendingConnectionAcquires() throws Exception {
        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();

        SdkAsyncHttpClient httpClient = AWSGeneralUtil.createAsyncHttpClient(builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        SdkAsyncHttpClient httpDefaultClient = NettyNioAsyncHttpClient.create();
        NettyConfiguration nettyDefaultConfiguration =
                TestUtil.getNettyConfiguration(httpDefaultClient);

        assertThat(nettyConfiguration.maxPendingConnectionAcquires())
                .isEqualTo(nettyDefaultConfiguration.maxPendingConnectionAcquires());
    }

    @Test
    void testCreateNettyAsyncHttpClientWithDefaultsReadTimeout() throws Exception {
        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();

        SdkAsyncHttpClient httpClient = AWSGeneralUtil.createAsyncHttpClient(builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        assertThat(nettyConfiguration.readTimeoutMillis()).isEqualTo(360_000);
    }

    @Test
    void testCreateNettyAsyncHttpClientWithDefaultsReapIdleConnections() throws Exception {
        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();

        SdkAsyncHttpClient httpClient = AWSGeneralUtil.createAsyncHttpClient(builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        SdkAsyncHttpClient httpDefaultClient = NettyNioAsyncHttpClient.create();
        NettyConfiguration nettyDefaultConfiguration =
                TestUtil.getNettyConfiguration(httpDefaultClient);

        assertThat(nettyConfiguration.reapIdleConnections())
                .isEqualTo(nettyDefaultConfiguration.reapIdleConnections());
    }

    @Test
    void testCreateNettyAsyncHttpClientWithDefaultsTcpKeepAlive() throws Exception {
        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();

        SdkAsyncHttpClient httpClient = AWSGeneralUtil.createAsyncHttpClient(builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        SdkAsyncHttpClient httpDefaultClient = NettyNioAsyncHttpClient.create();
        NettyConfiguration nettyDefaultConfiguration =
                TestUtil.getNettyConfiguration(httpDefaultClient);

        assertThat(nettyConfiguration.tcpKeepAlive())
                .isEqualTo(nettyDefaultConfiguration.tcpKeepAlive());
    }

    @Test
    void testCreateNettyAsyncHttpClientWithDefaultsTlsKeyManagersProvider() throws Exception {
        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();

        SdkAsyncHttpClient httpClient = AWSGeneralUtil.createAsyncHttpClient(builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        SdkAsyncHttpClient httpDefaultClient = NettyNioAsyncHttpClient.create();
        NettyConfiguration nettyDefaultConfiguration =
                TestUtil.getNettyConfiguration(httpDefaultClient);

        assertThat(nettyConfiguration.tlsKeyManagersProvider())
                .isEqualTo(nettyDefaultConfiguration.tlsKeyManagersProvider());
    }

    @Test
    void testCreateNettyAsyncHttpClientWithDefaultsTlsTrustManagersProvider() throws Exception {
        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();

        SdkAsyncHttpClient httpClient = AWSGeneralUtil.createAsyncHttpClient(builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        SdkAsyncHttpClient httpDefaultClient = NettyNioAsyncHttpClient.create();
        NettyConfiguration nettyDefaultConfiguration =
                TestUtil.getNettyConfiguration(httpDefaultClient);

        assertThat(nettyConfiguration.tlsTrustManagersProvider())
                .isEqualTo(nettyDefaultConfiguration.tlsTrustManagersProvider());
    }

    @Test
    void testCreateNettyAsyncHttpClientWithDefaultsTrustAllCertificates() throws Exception {
        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();

        SdkAsyncHttpClient httpClient = AWSGeneralUtil.createAsyncHttpClient(builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        assertThat(nettyConfiguration.trustAllCertificates()).isFalse();
    }

    @Test
    void testCreateNettyAsyncHttpClientWithDefaultsWriteTimeout() throws Exception {
        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();

        SdkAsyncHttpClient httpClient = AWSGeneralUtil.createAsyncHttpClient(builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        SdkAsyncHttpClient httpDefaultClient = NettyNioAsyncHttpClient.create();
        NettyConfiguration nettyDefaultConfiguration =
                TestUtil.getNettyConfiguration(httpDefaultClient);

        assertThat(nettyConfiguration.writeTimeoutMillis())
                .isEqualTo(nettyDefaultConfiguration.writeTimeoutMillis());
    }

    @Test
    void testCreateNettyAsyncHttpClientWithDefaultsProtocol() throws Exception {
        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();

        SdkAsyncHttpClient httpClient = AWSGeneralUtil.createAsyncHttpClient(builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        assertThat(nettyConfiguration.attribute(SdkHttpConfigurationOption.PROTOCOL))
                .isEqualTo(HTTP2);
    }

    @Test
    void testCreateNettyAsyncHttpClientReadTimeout() throws Exception {
        Duration readTimeout = Duration.ofMillis(1234);

        AttributeMap clientConfiguration =
                AttributeMap.builder()
                        .put(SdkHttpConfigurationOption.READ_TIMEOUT, readTimeout)
                        .build();

        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();
        SdkAsyncHttpClient httpClient =
                AWSGeneralUtil.createAsyncHttpClient(clientConfiguration, builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        assertThat(nettyConfiguration.readTimeoutMillis()).isEqualTo(readTimeout.toMillis());
    }

    @Test
    void testCreateNettyAsyncHttpClientTcpKeepAlive() throws Exception {
        boolean tcpKeepAlive = true;

        AttributeMap clientConfiguration =
                AttributeMap.builder()
                        .put(SdkHttpConfigurationOption.TCP_KEEPALIVE, tcpKeepAlive)
                        .build();

        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();
        SdkAsyncHttpClient httpClient =
                AWSGeneralUtil.createAsyncHttpClient(clientConfiguration, builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        assertThat(nettyConfiguration.tcpKeepAlive()).isEqualTo(tcpKeepAlive);
    }

    @Test
    void testCreateNettyAsyncHttpClientConnectionTimeout() throws Exception {
        Duration connectionTimeout = Duration.ofMillis(1000);

        AttributeMap clientConfiguration =
                AttributeMap.builder()
                        .put(SdkHttpConfigurationOption.CONNECTION_TIMEOUT, connectionTimeout)
                        .build();

        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();
        SdkAsyncHttpClient httpClient =
                AWSGeneralUtil.createAsyncHttpClient(clientConfiguration, builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        assertThat(nettyConfiguration.connectTimeoutMillis())
                .isEqualTo(connectionTimeout.toMillis());
    }

    @Test
    void testCreateNettyAsyncHttpClientMaxConcurrency() throws Exception {
        int maxConnections = 123;

        AttributeMap clientConfiguration =
                AttributeMap.builder()
                        .put(SdkHttpConfigurationOption.MAX_CONNECTIONS, maxConnections)
                        .build();

        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();
        SdkAsyncHttpClient httpClient =
                AWSGeneralUtil.createAsyncHttpClient(clientConfiguration, builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        assertThat(nettyConfiguration.maxConnections()).isEqualTo(maxConnections);
    }

    @Test
    void testCreateNettyAsyncHttpClientWriteTimeout() throws Exception {
        Duration writeTimeout = Duration.ofMillis(3000);

        AttributeMap clientConfiguration =
                AttributeMap.builder()
                        .put(SdkHttpConfigurationOption.WRITE_TIMEOUT, writeTimeout)
                        .build();

        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();
        SdkAsyncHttpClient httpClient =
                AWSGeneralUtil.createAsyncHttpClient(clientConfiguration, builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        assertThat(nettyConfiguration.writeTimeoutMillis()).isEqualTo(writeTimeout.toMillis());
    }

    @Test
    void testCreateNettyAsyncHttpClientConnectionMaxIdleTime() throws Exception {
        Duration maxIdleTime = Duration.ofMillis(2000);

        AttributeMap clientConfiguration =
                AttributeMap.builder()
                        .put(SdkHttpConfigurationOption.CONNECTION_MAX_IDLE_TIMEOUT, maxIdleTime)
                        .build();

        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();
        SdkAsyncHttpClient httpClient =
                AWSGeneralUtil.createAsyncHttpClient(clientConfiguration, builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        assertThat(nettyConfiguration.idleTimeoutMillis()).isEqualTo(maxIdleTime.toMillis());
    }

    @Test
    void testCreateNettyAsyncHttpClientIdleConnectionReaper() throws Exception {
        boolean reapIdleConnections = false;

        AttributeMap clientConfiguration =
                AttributeMap.builder()
                        .put(SdkHttpConfigurationOption.REAP_IDLE_CONNECTIONS, reapIdleConnections)
                        .build();

        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();
        SdkAsyncHttpClient httpClient =
                AWSGeneralUtil.createAsyncHttpClient(clientConfiguration, builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        assertThat(nettyConfiguration.reapIdleConnections()).isEqualTo(reapIdleConnections);
    }

    @Test
    void testCreateNettyAsyncHttpClientIdleConnectionTtl() throws Exception {
        Duration connectionTtl = Duration.ofMillis(5000);

        AttributeMap clientConfiguration =
                AttributeMap.builder()
                        .put(SdkHttpConfigurationOption.CONNECTION_TIME_TO_LIVE, connectionTtl)
                        .build();

        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();
        SdkAsyncHttpClient httpClient =
                AWSGeneralUtil.createAsyncHttpClient(clientConfiguration, builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        assertThat(nettyConfiguration.connectionTtlMillis()).isEqualTo(connectionTtl.toMillis());
    }

    @Test
    void testCreateNettyAsyncHttpClientTrustAllCertificates() throws Exception {
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

        assertThat(nettyConfiguration.trustAllCertificates()).isEqualTo(trustAllCertificates);
    }

    @Test
    void testCreateNettyAsyncHttpClientHttpVersion() throws Exception {
        Protocol httpVersion = HTTP1_1;

        AttributeMap clientConfiguration =
                AttributeMap.builder()
                        .put(SdkHttpConfigurationOption.PROTOCOL, httpVersion)
                        .build();

        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();
        SdkAsyncHttpClient httpClient =
                AWSGeneralUtil.createAsyncHttpClient(clientConfiguration, builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        assertThat(nettyConfiguration.attribute(SdkHttpConfigurationOption.PROTOCOL))
                .isEqualTo(httpVersion);
    }

    @Test
    void testGetRegion() {
        Region region = AWSGeneralUtil.getRegion(TestUtil.properties(AWS_REGION, "eu-west-2"));

        assertThat(region).isEqualTo(Region.EU_WEST_2);
    }

    @Test
    void testValidRegion() {
        assertThat(AWSGeneralUtil.isValidRegion(Region.of("us-east-1"))).isTrue();
        assertThat(AWSGeneralUtil.isValidRegion(Region.of("us-gov-west-1"))).isTrue();
        assertThat(AWSGeneralUtil.isValidRegion(Region.of("us-isob-east-1"))).isTrue();
        assertThat(AWSGeneralUtil.isValidRegion(Region.of("aws-global"))).isTrue();
        assertThat(AWSGeneralUtil.isValidRegion(Region.of("aws-iso-global"))).isTrue();
        assertThat(AWSGeneralUtil.isValidRegion(Region.of("aws-iso-b-global"))).isTrue();
    }

    @Test
    void testInvalidRegion() {
        assertThat(AWSGeneralUtil.isValidRegion(Region.of("unstructured-string"))).isFalse();
    }

    @Test
    void testUnrecognizableAwsRegionInConfig() {

        Properties testConfig = TestUtil.properties(AWSConfigConstants.AWS_REGION, "wrongRegionId");
        testConfig.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
        testConfig.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");

        assertThatThrownBy(() -> AWSGeneralUtil.validateAwsConfiguration(testConfig))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid AWS region");
    }

    @Test
    void testCredentialProviderTypeSetToBasicButNoCredentialSetInConfig() {
        Properties testConfig = TestUtil.properties(AWSConfigConstants.AWS_REGION, "us-east-1");
        testConfig.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");

        assertThatThrownBy(() -> AWSGeneralUtil.validateAwsConfiguration(testConfig))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Please set values for AWS Access Key ID ('"
                                + AWSConfigConstants.AWS_ACCESS_KEY_ID
                                + "') "
                                + "and Secret Key ('"
                                + AWSConfigConstants.AWS_SECRET_ACCESS_KEY
                                + "') when using the BASIC AWS credential provider type.");
    }

    @Test
    void testUnrecognizableCredentialProviderTypeInConfig() {
        Properties testConfig = TestUtil.getStandardProperties();
        testConfig.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "wrongProviderType");

        assertThatThrownBy(() -> AWSGeneralUtil.validateAwsConfiguration(testConfig))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid AWS Credential Provider Type");
    }

    @Test
    void testMissingWebIdentityTokenFileInCredentials() {
        Properties properties = TestUtil.getStandardProperties();
        properties.setProperty(AWS_CREDENTIALS_PROVIDER, "WEB_IDENTITY_TOKEN");

        assertThatThrownBy(() -> AWSGeneralUtil.validateAwsCredentials(properties))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Either the environment variable AWS_WEB_IDENTITY_TOKEN_FILE or the javaproperty aws.webIdentityTokenFile must be set.");
    }

    @Test
    void testMissingEnvironmentVariableCredentials() {
        Properties properties = TestUtil.getStandardProperties();
        properties.setProperty(AWS_CREDENTIALS_PROVIDER, "ENV_VAR");

        assertThatThrownBy(() -> AWSGeneralUtil.validateAwsCredentials(properties))
                .isInstanceOf(SdkClientException.class)
                .hasMessageContaining(
                        "Access key must be specified either via environment variable");
    }

    @Test
    void testFailedSystemPropertiesCredentialsValidationsOnMissingAccessKey() {
        Properties properties = TestUtil.getStandardProperties();
        properties.setProperty(AWS_CREDENTIALS_PROVIDER, "SYS_PROP");

        assertThatThrownBy(() -> AWSGeneralUtil.validateAwsCredentials(properties))
                .isInstanceOf(SdkClientException.class)
                .hasMessageContaining(
                        "Access key must be specified either via environment variable (AWS_ACCESS_KEY_ID) or system property (aws.accessKeyId)");
    }

    @Test
    void testFailedSystemPropertiesCredentialsValidationsOnMissingSecretKey() {
        System.setProperty("aws.accessKeyId", "accesKeyId");
        Properties properties = TestUtil.getStandardProperties();
        properties.setProperty(AWS_CREDENTIALS_PROVIDER, "SYS_PROP");

        assertThatThrownBy(() -> AWSGeneralUtil.validateAwsCredentials(properties))
                .isInstanceOf(SdkClientException.class)
                .hasMessageContaining(
                        "Secret key must be specified either via environment variable (AWS_SECRET_ACCESS_KEY) or system property (aws.secretAccessKey)");
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
