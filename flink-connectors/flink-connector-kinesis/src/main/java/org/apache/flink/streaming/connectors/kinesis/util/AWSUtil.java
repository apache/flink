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

import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants.CredentialProvider;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.ClientConfigurationFactory;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisClient;

import java.util.Properties;

/**
 * Some utilities specific to Amazon Web Service.
 */
public class AWSUtil {

	/**
	 * Creates an Amazon Kinesis Client.
	 * @param configProps configuration properties containing the access key, secret key, and region
	 * @return a new Amazon Kinesis Client
	 */
	public static AmazonKinesisClient createKinesisClient(Properties configProps) {
		// set a Flink-specific user agent
		ClientConfiguration awsClientConfig = new ClientConfigurationFactory().getConfig();
		awsClientConfig.setUserAgent("Apache Flink " + EnvironmentInformation.getVersion() +
			" (" + EnvironmentInformation.getRevisionInformation().commitId + ") Kinesis Connector");

		// utilize automatic refreshment of credentials by directly passing the AWSCredentialsProvider
		AmazonKinesisClient client = new AmazonKinesisClient(
			AWSUtil.getCredentialsProvider(configProps), awsClientConfig);

		client.setRegion(Region.getRegion(Regions.fromName(configProps.getProperty(AWSConfigConstants.AWS_REGION))));
		if (configProps.containsKey(AWSConfigConstants.AWS_ENDPOINT)) {
			client.setEndpoint(configProps.getProperty(AWSConfigConstants.AWS_ENDPOINT));
		}
		return client;
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
