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
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants.CredentialProvider;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.ClientConfigurationFactory;
import com.amazonaws.ProxyAuthenticationMethod;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;

import java.util.Properties;
import java.util.Vector;

/**
 * Some utilities specific to Amazon Web Service.
 */
@Internal
public class AWSUtil {
	/** Used for formatting Flink-specific user agent string when creating Kinesis client. */
	private static final String USER_AGENT_FORMAT = "Apache Flink %s (%s) Kinesis Connector";

	/**
	 * Creates an AmazonKinesis client.
	 * @param configProps configuration properties containing the access key, secret key, and region
	 * @return a new AmazonKinesis client
	 */
	public static AmazonKinesis createKinesisClient(Properties configProps) {
		// set a Flink-specific user agent
		ClientConfiguration awsClientConfig = new ClientConfigurationFactory().getConfig()
				.withUserAgentPrefix(String.format(USER_AGENT_FORMAT,
														EnvironmentInformation.getVersion(),
														EnvironmentInformation.getRevisionInformation().commitId));

		if (configProps.containsKey(AWSConfigConstants.AWS_PROXY_SERVER) &&
				configProps.containsKey(AWSConfigConstants.AWS_PROXY_PORT)) {
			awsClientConfig.setProxyHost(configProps.getProperty(AWSConfigConstants.AWS_PROXY_SERVER));
			awsClientConfig.setProxyPort(Integer.parseInt(configProps.getProperty(AWSConfigConstants.AWS_PROXY_PORT)));

			if (configProps.containsKey(AWSConfigConstants.AWS_PROXY_AUTH_USER) &&
					configProps.containsKey(AWSConfigConstants.AWS_PROXY_AUTH_PASSWORD)) {
				awsClientConfig.setProxyUsername(configProps.getProperty(AWSConfigConstants.AWS_PROXY_AUTH_USER));
				awsClientConfig.setProxyPassword(configProps.getProperty(AWSConfigConstants.AWS_PROXY_AUTH_PASSWORD));

				if (configProps.containsKey(AWSConfigConstants.AWS_PROXY_AUTH_METHODS)) {
					Vector<ProxyAuthenticationMethod> prxAuthMeth = new Vector<ProxyAuthenticationMethod>();
					for (String meth: configProps.getProperty(AWSConfigConstants.AWS_PROXY_AUTH_METHODS).split("[ ,;:]")) {
						switch (meth.toUpperCase()) {
						case "NTML":
							prxAuthMeth.add(ProxyAuthenticationMethod.NTLM);
							if (configProps.containsKey(AWSConfigConstants.AWS_PROXY_AUTH_NTML_DOMAIN)) {
								awsClientConfig.setProxyDomain(configProps.getProperty(AWSConfigConstants.AWS_PROXY_AUTH_NTML_DOMAIN));
							}
							if (configProps.containsKey(AWSConfigConstants.AWS_PROXY_AUTH_NTML_WORKSTATION)) {
								awsClientConfig.setProxyDomain(configProps.getProperty(AWSConfigConstants.AWS_PROXY_AUTH_NTML_WORKSTATION));
							}
							break;
						case "SPNEGO":
							prxAuthMeth.add(ProxyAuthenticationMethod.SPNEGO);
							break;
						case "DIGEST":
							prxAuthMeth.add(ProxyAuthenticationMethod.DIGEST);
							break;
						case "KERBEROS":
							prxAuthMeth.add(ProxyAuthenticationMethod.KERBEROS);
							break;
						case "BASIC":
							prxAuthMeth.add(ProxyAuthenticationMethod.BASIC);
							break;
						}
					}
					awsClientConfig.setProxyAuthenticationMethods(prxAuthMeth);
				}
			}
		}
		// utilize automatic refreshment of credentials by directly passing the AWSCredentialsProvider
		AmazonKinesisClientBuilder builder = AmazonKinesisClientBuilder.standard()
				.withCredentials(AWSUtil.getCredentialsProvider(configProps))
				.withClientConfiguration(awsClientConfig)
				.withRegion(Regions.fromName(configProps.getProperty(AWSConfigConstants.AWS_REGION)));

		if (configProps.containsKey(AWSConfigConstants.AWS_ENDPOINT)) {
			// Set signingRegion as null, to facilitate mocking Kinesis for local tests
			builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
													configProps.getProperty(AWSConfigConstants.AWS_ENDPOINT),
													null));
		}
		return builder.build();
	}

	/**
	 * Return a {@link AWSCredentialsProvider} instance corresponding to the configuration properties.
	 *
	 * @param configProps the configuration properties
	 * @return The corresponding AWS Credentials Provider instance
	 */
	public static AWSCredentialsProvider getCredentialsProvider(final Properties configProps) {
		CredentialProvider credentialProviderType;
		if (!configProps.containsKey(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER)) {
			if (configProps.containsKey(AWSConfigConstants.AWS_ACCESS_KEY_ID)
				&& configProps.containsKey(AWSConfigConstants.AWS_SECRET_ACCESS_KEY)) {
				// if the credential provider type is not specified, but the Access Key ID and Secret Key are given, it will default to BASIC
				credentialProviderType = CredentialProvider.BASIC;
			} else {
				// if the credential provider type is not specified, it will default to AUTO
				credentialProviderType = CredentialProvider.AUTO;
			}
		} else {
			credentialProviderType = CredentialProvider.valueOf(configProps.getProperty(
				AWSConfigConstants.AWS_CREDENTIALS_PROVIDER));
		}

		AWSCredentialsProvider credentialsProvider;

		switch (credentialProviderType) {
			case ENV_VAR:
				credentialsProvider = new EnvironmentVariableCredentialsProvider();
				break;
			case SYS_PROP:
				credentialsProvider = new SystemPropertiesCredentialsProvider();
				break;
			case PROFILE:
				String profileName = configProps.getProperty(
					AWSConfigConstants.AWS_PROFILE_NAME, null);
				String profileConfigPath = configProps.getProperty(
					AWSConfigConstants.AWS_PROFILE_PATH, null);
				credentialsProvider = (profileConfigPath == null)
					? new ProfileCredentialsProvider(profileName)
					: new ProfileCredentialsProvider(profileConfigPath, profileName);
				break;
			case BASIC:
				credentialsProvider = new AWSCredentialsProvider() {
					@Override
					public AWSCredentials getCredentials() {
						return new BasicAWSCredentials(
							configProps.getProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID),
							configProps.getProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY));
					}

					@Override
					public void refresh() {
						// do nothing
					}
				};
				break;
			default:
			case AUTO:
				credentialsProvider = new DefaultAWSCredentialsProviderChain();
		}

		return credentialsProvider;
	}

	/**
	 * Checks whether or not a region ID is valid.
	 *
	 * @param region The AWS region ID to check
	 * @return true if the supplied region ID is valid, false otherwise
	 */
	public static boolean isValidRegion(String region) {
		try {
			Regions.fromName(region.toLowerCase());
		} catch (IllegalArgumentException e) {
			return false;
		}
		return true;
	}
}
