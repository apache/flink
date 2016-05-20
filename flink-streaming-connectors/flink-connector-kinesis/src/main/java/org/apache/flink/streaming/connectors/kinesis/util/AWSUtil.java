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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import org.apache.flink.streaming.connectors.kinesis.config.KinesisConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.CredentialProviderType;

import java.util.Properties;

/**
 * Some utilities specific to Amazon Web Service.
 */
public class AWSUtil {

	/**
	 * Return a {@link AWSCredentialsProvider} instance corresponding to the configuration properties.
	 *
	 * @param configProps the configuration properties
	 * @return The corresponding AWS Credentials Provider instance
	 */
	public static AWSCredentialsProvider getCredentialsProvider(final Properties configProps) {
		CredentialProviderType credentialProviderType = CredentialProviderType.valueOf(configProps.getProperty(
			KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_TYPE, CredentialProviderType.BASIC.toString()));

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
					KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_PROFILE_NAME, null);
				String profileConfigPath = configProps.getProperty(
					KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_PROFILE_PATH, null);
				credentialsProvider = (profileConfigPath == null)
					? new ProfileCredentialsProvider(profileName)
					: new ProfileCredentialsProvider(profileConfigPath, profileName);
				break;
			default:
			case BASIC:
				credentialsProvider = new AWSCredentialsProvider() {
					@Override
					public AWSCredentials getCredentials() {
						return new BasicAWSCredentials(
							configProps.getProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_ACCESSKEYID),
							configProps.getProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_SECRETKEY));
					}

					@Override
					public void refresh() {
						// do nothing
					}
				};
		}

		return credentialsProvider;
	}

	/**
	 * Checks whether or not a region ID is valid
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
