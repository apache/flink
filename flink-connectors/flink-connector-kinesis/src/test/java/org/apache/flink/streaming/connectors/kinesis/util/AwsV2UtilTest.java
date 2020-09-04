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
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.ClientConfigurationFactory;
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
import software.amazon.awssdk.http.nio.netty.Http2Configuration;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClientBuilder;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.net.URI;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants.AWS_CREDENTIALS_PROVIDER;
import static org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants.AWS_REGION;
import static org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants.roleArn;
import static org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants.roleSessionName;
import static org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants.webIdentityTokenFile;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.DEFAULT_EFO_HTTP_CLIENT_MAX_CONURRENCY;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFORegistrationType.EAGER;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFORegistrationType.LAZY;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFORegistrationType.NONE;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFO_HTTP_CLIENT_MAX_CONCURRENCY;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.RECORD_PUBLISHER_TYPE;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.RecordPublisherType.EFO;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.RecordPublisherType.POLLING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.awssdk.http.Protocol.HTTP2;

/**
 * Tests for {@link AwsV2Util}.
 */
public class AwsV2UtilTest {

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

		WebIdentityTokenFileCredentialsProvider.Builder builder = mockWebIdentityTokenFileCredentialsProviderBuilder();

		AwsV2Util.getWebIdentityTokenFileCredentialsProvider(builder, properties, AWS_CREDENTIALS_PROVIDER);

		verify(builder).roleArn("roleArn");
		verify(builder).roleSessionName("roleSessionName");
		verify(builder, never()).webIdentityTokenFile(any());
	}

	@Test
	public void testGetWebIdentityTokenFileCredentialsProviderWithWebIdentityFile() {
		Properties properties = properties(AWS_CREDENTIALS_PROVIDER, "WEB_IDENTITY_TOKEN");
		properties.setProperty(webIdentityTokenFile(AWS_CREDENTIALS_PROVIDER), "webIdentityTokenFile");

		WebIdentityTokenFileCredentialsProvider.Builder builder = mockWebIdentityTokenFileCredentialsProviderBuilder();

		AwsV2Util.getWebIdentityTokenFileCredentialsProvider(builder, properties, AWS_CREDENTIALS_PROVIDER);

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
		verify(properties).getProperty(AWSConfigConstants.roleSessionName(AWS_CREDENTIALS_PROVIDER));
		verify(properties).getProperty(AWSConfigConstants.externalId(AWS_CREDENTIALS_PROVIDER));
		verify(properties).getProperty(AWS_REGION);
	}

	@Test
	public void testGetCredentialsProviderBasic() {
		Properties properties = properties(AWS_CREDENTIALS_PROVIDER, "BASIC");
		properties.setProperty(AWSConfigConstants.accessKeyId(AWS_CREDENTIALS_PROVIDER), "ak");
		properties.setProperty(AWSConfigConstants.secretKey(AWS_CREDENTIALS_PROVIDER), "sk");

		AwsCredentials credentials = AwsV2Util.getCredentialsProvider(properties).resolveCredentials();

		assertEquals("ak", credentials.accessKeyId());
		assertEquals("sk", credentials.secretAccessKey());
	}

	@Test
	public void testGetCredentialsProviderProfile() {
		Properties properties = properties(AWS_CREDENTIALS_PROVIDER, "PROFILE");
		properties.put(AWSConfigConstants.profileName(AWS_CREDENTIALS_PROVIDER), "default");
		properties.put(AWSConfigConstants.profilePath(AWS_CREDENTIALS_PROVIDER), "src/test/resources/profile");

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
		properties.setProperty(AWSConfigConstants.profilePath(AWS_CREDENTIALS_PROVIDER), "src/test/resources/profile");

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
	public void testCreateKinesisAsyncClient() {
		Properties properties = properties(AWS_REGION, "eu-west-2");
		KinesisAsyncClientBuilder builder = mockKinesisAsyncClientBuilder();
		ClientOverrideConfiguration clientOverrideConfiguration = ClientOverrideConfiguration.builder().build();
		SdkAsyncHttpClient httpClient = NettyNioAsyncHttpClient.builder().build();

		AwsV2Util.createKinesisAsyncClient(properties, builder, httpClient, clientOverrideConfiguration);

		verify(builder).overrideConfiguration(clientOverrideConfiguration);
		verify(builder).httpClient(httpClient);
		verify(builder).region(Region.of("eu-west-2"));
		verify(builder).credentialsProvider(argThat(cp -> cp instanceof DefaultCredentialsProvider));
		verify(builder, never()).endpointOverride(any());
	}

	@Test
	public void testCreateKinesisAsyncClientWithEndpointOverride() {
		Properties properties = properties(AWS_REGION, "eu-west-2");
		properties.setProperty(ConsumerConfigConstants.AWS_ENDPOINT, "https://localhost");

		KinesisAsyncClientBuilder builder = mockKinesisAsyncClientBuilder();
		ClientOverrideConfiguration clientOverrideConfiguration = ClientOverrideConfiguration.builder().build();
		SdkAsyncHttpClient httpClient = NettyNioAsyncHttpClient.builder().build();

		AwsV2Util.createKinesisAsyncClient(properties, builder, httpClient, clientOverrideConfiguration);

		verify(builder).endpointOverride(URI.create("https://localhost"));
	}

	@Test
	public void testCreateNettyHttpClientWithDefaults() {
		ClientConfiguration clientConfiguration = new ClientConfigurationFactory().getConfig();
		NettyNioAsyncHttpClient.Builder builder = mockHttpClientBuilder();

		AwsV2Util.createHttpClient(clientConfiguration, builder, new Properties());

		verify(builder).build();
		verify(builder).maxConcurrency(DEFAULT_EFO_HTTP_CLIENT_MAX_CONURRENCY);
		verify(builder).connectionTimeout(Duration.ofSeconds(10));
		verify(builder).writeTimeout(Duration.ofSeconds(50));
		verify(builder).connectionMaxIdleTime(Duration.ofMinutes(1));
		verify(builder).useIdleConnectionReaper(true);
		verify(builder).protocol(HTTP2);
		verify(builder, never()).connectionTimeToLive(any());
	}

	@Test
	public void testCreateNettyHttpClientConnectionTimeout() {
		ClientConfiguration clientConfiguration = new ClientConfigurationFactory().getConfig();
		clientConfiguration.setConnectionTimeout(1000);

		NettyNioAsyncHttpClient.Builder builder = mockHttpClientBuilder();

		AwsV2Util.createHttpClient(clientConfiguration, builder, new Properties());

		verify(builder).connectionTimeout(Duration.ofSeconds(1));
	}

	@Test
	public void testCreateNettyHttpClientMaxConcurrency() {
		Properties clientConfiguration = new Properties();
		clientConfiguration.setProperty(EFO_HTTP_CLIENT_MAX_CONCURRENCY, "123");

		NettyNioAsyncHttpClient.Builder builder = mockHttpClientBuilder();

		AwsV2Util.createHttpClient(new ClientConfigurationFactory().getConfig(), builder, clientConfiguration);

		verify(builder).maxConcurrency(123);
	}

	@Test
	public void testCreateNettyHttpClientWriteTimeout() {
		ClientConfiguration clientConfiguration = new ClientConfigurationFactory().getConfig();
		clientConfiguration.setSocketTimeout(3000);

		NettyNioAsyncHttpClient.Builder builder = mockHttpClientBuilder();

		AwsV2Util.createHttpClient(clientConfiguration, builder, new Properties());

		verify(builder).writeTimeout(Duration.ofSeconds(3));
	}

	@Test
	public void testCreateNettyHttpClientConnectionMaxIdleTime() {
		ClientConfiguration clientConfiguration = new ClientConfigurationFactory().getConfig();
		clientConfiguration.setConnectionMaxIdleMillis(2000);

		NettyNioAsyncHttpClient.Builder builder = mockHttpClientBuilder();

		AwsV2Util.createHttpClient(clientConfiguration, builder, new Properties());

		verify(builder).connectionMaxIdleTime(Duration.ofSeconds(2));
	}

	@Test
	public void testCreateNettyHttpClientIdleConnectionReaper() {
		ClientConfiguration clientConfiguration = new ClientConfigurationFactory().getConfig();
		clientConfiguration.setUseReaper(false);

		NettyNioAsyncHttpClient.Builder builder = mockHttpClientBuilder();

		AwsV2Util.createHttpClient(clientConfiguration, builder, new Properties());

		verify(builder).useIdleConnectionReaper(false);
	}

	@Test
	public void testCreateNettyHttpClientIdleConnectionTtl() {
		ClientConfiguration clientConfiguration = new ClientConfigurationFactory().getConfig();
		clientConfiguration.setConnectionTTL(5000);

		NettyNioAsyncHttpClient.Builder builder = mockHttpClientBuilder();

		AwsV2Util.createHttpClient(clientConfiguration, builder, new Properties());

		verify(builder).connectionTimeToLive(Duration.ofSeconds(5));
	}

	@Test
	public void testClientOverrideConfigurationWithDefaults() {
		ClientConfiguration clientConfiguration = new ClientConfigurationFactory().getConfig();
		ClientOverrideConfiguration.Builder builder = mockClientOverrideConfigurationBuilder();

		AwsV2Util.createClientOverrideConfiguration(clientConfiguration, builder);

		verify(builder).build();
		verify(builder).putAdvancedOption(SdkAdvancedClientOption.USER_AGENT_PREFIX, AWSUtil.formatFlinkUserAgentPrefix());
		verify(builder).putAdvancedOption(SdkAdvancedClientOption.USER_AGENT_SUFFIX, null);
		verify(builder, never()).apiCallAttemptTimeout(any());
		verify(builder, never()).apiCallTimeout(any());
	}

	@Test
	public void testClientOverrideConfigurationUserAgentSuffix() {
		ClientConfiguration clientConfiguration = new ClientConfigurationFactory().getConfig();
		clientConfiguration.setUserAgentSuffix("suffix");

		ClientOverrideConfiguration.Builder builder = mockClientOverrideConfigurationBuilder();

		AwsV2Util.createClientOverrideConfiguration(clientConfiguration, builder);

		verify(builder).putAdvancedOption(SdkAdvancedClientOption.USER_AGENT_SUFFIX, "suffix");
	}

	@Test
	public void testClientOverrideConfigurationApiCallAttemptTimeout() {
		ClientConfiguration clientConfiguration = new ClientConfigurationFactory().getConfig();
		clientConfiguration.setRequestTimeout(500);

		ClientOverrideConfiguration.Builder builder = mockClientOverrideConfigurationBuilder();

		AwsV2Util.createClientOverrideConfiguration(clientConfiguration, builder);

		verify(builder).apiCallAttemptTimeout(Duration.ofMillis(500));
	}

	@Test
	public void testClientOverrideConfigurationApiCallTimeout() {
		ClientConfiguration clientConfiguration = new ClientConfigurationFactory().getConfig();
		clientConfiguration.setClientExecutionTimeout(600);

		ClientOverrideConfiguration.Builder builder = mockClientOverrideConfigurationBuilder();

		AwsV2Util.createClientOverrideConfiguration(clientConfiguration, builder);

		verify(builder).apiCallTimeout(Duration.ofMillis(600));
	}

	private Properties properties(final String key, final String value) {
		Properties properties = new Properties();
		properties.setProperty(key, value);
		return properties;
	}

	private KinesisAsyncClientBuilder mockKinesisAsyncClientBuilder() {
		KinesisAsyncClientBuilder builder = mock(KinesisAsyncClientBuilder.class);
		when(builder.overrideConfiguration(any(ClientOverrideConfiguration.class))).thenReturn(builder);
		when(builder.httpClient(any())).thenReturn(builder);
		when(builder.credentialsProvider(any())).thenReturn(builder);
		when(builder.region(any())).thenReturn(builder);

		return builder;
	}

	private NettyNioAsyncHttpClient.Builder mockHttpClientBuilder() {
		NettyNioAsyncHttpClient.Builder builder = mock(NettyNioAsyncHttpClient.Builder.class);
		when(builder.maxConcurrency(anyInt())).thenReturn(builder);
		when(builder.connectionTimeout(any())).thenReturn(builder);
		when(builder.writeTimeout(any())).thenReturn(builder);
		when(builder.connectionMaxIdleTime(any())).thenReturn(builder);
		when(builder.useIdleConnectionReaper(anyBoolean())).thenReturn(builder);
		when(builder.connectionAcquisitionTimeout(any())).thenReturn(builder);
		when(builder.protocol(any())).thenReturn(builder);
		when(builder.http2Configuration(any(Http2Configuration.class))).thenReturn(builder);

		return builder;
	}

	private ClientOverrideConfiguration.Builder mockClientOverrideConfigurationBuilder() {
		ClientOverrideConfiguration.Builder builder = mock(ClientOverrideConfiguration.Builder.class);
		when(builder.putAdvancedOption(any(), any())).thenReturn(builder);
		when(builder.apiCallAttemptTimeout(any())).thenReturn(builder);
		when(builder.apiCallTimeout(any())).thenReturn(builder);

		return builder;
	}

	private WebIdentityTokenFileCredentialsProvider.Builder mockWebIdentityTokenFileCredentialsProviderBuilder() {
		WebIdentityTokenFileCredentialsProvider.Builder builder = mock(WebIdentityTokenFileCredentialsProvider.Builder.class);
		when(builder.roleArn(any())).thenReturn(builder);
		when(builder.roleSessionName(any())).thenReturn(builder);
		when(builder.webIdentityTokenFile(any())).thenReturn(builder);

		return builder;
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

		prop.setProperty(ConsumerConfigConstants.EFO_REGISTRATION_TYPE, EAGER.name());
		assertTrue(AwsV2Util.isEagerEfoRegistrationType(prop));

		prop.setProperty(ConsumerConfigConstants.EFO_REGISTRATION_TYPE, LAZY.name());
		assertFalse(AwsV2Util.isEagerEfoRegistrationType(prop));

		prop.setProperty(ConsumerConfigConstants.EFO_REGISTRATION_TYPE, NONE.name());
		assertFalse(AwsV2Util.isEagerEfoRegistrationType(prop));
	}

	@Test
	public void testIsLazyEfoRegistrationType() {
		Properties prop = new Properties();
		assertTrue(AwsV2Util.isLazyEfoRegistrationType(prop));

		prop.setProperty(ConsumerConfigConstants.EFO_REGISTRATION_TYPE, EAGER.name());
		assertFalse(AwsV2Util.isLazyEfoRegistrationType(prop));

		prop.setProperty(ConsumerConfigConstants.EFO_REGISTRATION_TYPE, LAZY.name());
		assertTrue(AwsV2Util.isLazyEfoRegistrationType(prop));

		prop.setProperty(ConsumerConfigConstants.EFO_REGISTRATION_TYPE, NONE.name());
		assertFalse(AwsV2Util.isLazyEfoRegistrationType(prop));
	}

	@Test
	public void testIsNoneEfoRegistrationType() {
		Properties prop = new Properties();
		assertFalse(AwsV2Util.isNoneEfoRegistrationType(prop));

		prop.setProperty(ConsumerConfigConstants.EFO_REGISTRATION_TYPE, EAGER.name());
		assertFalse(AwsV2Util.isNoneEfoRegistrationType(prop));

		prop.setProperty(ConsumerConfigConstants.EFO_REGISTRATION_TYPE, LAZY.name());
		assertFalse(AwsV2Util.isNoneEfoRegistrationType(prop));

		prop.setProperty(ConsumerConfigConstants.EFO_REGISTRATION_TYPE, NONE.name());
		assertTrue(AwsV2Util.isNoneEfoRegistrationType(prop));
	}

	@Test
	public void testIsRecoverableExceptionForRecoverable() {
		Exception recoverable = LimitExceededException.builder().build();
		assertTrue(AwsV2Util.isRecoverableException(new ExecutionException(recoverable)));
	}

	@Test
	public void testIsRecoverableExceptionForNonRecoverable() {
		Exception nonRecoverable = new IllegalArgumentException("abc");
		assertFalse(AwsV2Util.isRecoverableException(new ExecutionException(nonRecoverable)));
	}

	@Test
	public void testIsRecoverableExceptionForRuntimeExceptionWrappingRecoverable() {
		Exception recoverable = LimitExceededException.builder().build();
		Exception runtime = new RuntimeException("abc", recoverable);
		assertTrue(AwsV2Util.isRecoverableException(runtime));
	}

	@Test
	public void testIsRecoverableExceptionForRuntimeExceptionWrappingNonRecoverable() {
		Exception nonRecoverable = new IllegalArgumentException("abc");
		Exception runtime = new RuntimeException("abc", nonRecoverable);
		assertFalse(AwsV2Util.isRecoverableException(runtime));
	}

	@Test
	public void testIsRecoverableExceptionForNullCause() {
		Exception nonRecoverable = new IllegalArgumentException("abc");
		assertFalse(AwsV2Util.isRecoverableException(nonRecoverable));
	}

}
