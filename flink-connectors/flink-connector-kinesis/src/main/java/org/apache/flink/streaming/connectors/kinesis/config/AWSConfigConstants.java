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

package org.apache.flink.streaming.connectors.kinesis.config;

import org.apache.flink.annotation.PublicEvolving;

import com.amazonaws.auth.AWSCredentialsProvider;

/**
 * Configuration keys for AWS service usage.
 */
@PublicEvolving
public class AWSConfigConstants {

	/**
	 * Possible configuration values for the type of credential provider to use when accessing AWS Kinesis.
	 * Internally, a corresponding implementation of {@link AWSCredentialsProvider} will be used.
	 */
	public enum CredentialProvider {

		/** Look for the environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY to create AWS credentials. */
		ENV_VAR,

		/** Look for Java system properties aws.accessKeyId and aws.secretKey to create AWS credentials. */
		SYS_PROP,

		/** Use a AWS credentials profile file to create the AWS credentials. */
		PROFILE,

		/** Simply create AWS credentials by supplying the AWS access key ID and AWS secret key in the configuration properties. */
		BASIC,

		/** Create AWS credentials by assuming a role. The credentials for assuming the role must be supplied. **/
		ASSUME_ROLE,

		/** Use AWS WebIdentityToken in order to assume a role. A token file and role details can be supplied as configuration or environment variables. **/
		WEB_IDENTITY_TOKEN,

		/** A credentials provider chain will be used that searches for credentials in this order: ENV_VARS, SYS_PROPS, WEB_IDENTITY_TOKEN, PROFILE in the AWS instance metadata. **/
		AUTO,
	}

	/** The AWS region of the Kinesis streams to be pulled ("us-east-1" is used if not set). */
	public static final String AWS_REGION = "aws.region";

	/** The credential provider type to use when AWS credentials are required (BASIC is used if not set). */
	public static final String AWS_CREDENTIALS_PROVIDER = "aws.credentials.provider";

	/** The AWS access key ID to use when setting credentials provider type to BASIC. */
	public static final String AWS_ACCESS_KEY_ID = accessKeyId(AWS_CREDENTIALS_PROVIDER);

	/** The AWS secret key to use when setting credentials provider type to BASIC. */
	public static final String AWS_SECRET_ACCESS_KEY = secretKey(AWS_CREDENTIALS_PROVIDER);

	/** Optional configuration for profile path if credential provider type is set to be PROFILE. */
	public static final String AWS_PROFILE_PATH = profilePath(AWS_CREDENTIALS_PROVIDER);

	/** Optional configuration for profile name if credential provider type is set to be PROFILE. */
	public static final String AWS_PROFILE_NAME = profileName(AWS_CREDENTIALS_PROVIDER);

	/** The role ARN to use when credential provider type is set to ASSUME_ROLE or WEB_IDENTITY_TOKEN. */
	public static final String AWS_ROLE_ARN = roleArn(AWS_CREDENTIALS_PROVIDER);

	/** The role session name to use when credential provider type is set to ASSUME_ROLE or WEB_IDENTITY_TOKEN. */
	public static final String AWS_ROLE_SESSION_NAME = roleSessionName(AWS_CREDENTIALS_PROVIDER);

	/** The external ID to use when credential provider type is set to ASSUME_ROLE. */
	public static final String AWS_ROLE_EXTERNAL_ID = externalId(AWS_CREDENTIALS_PROVIDER);

	/** The absolute path to the web identity token file that should be used if provider type is set to WEB_IDENTITY_TOKEN. */
	public static final String AWS_WEB_IDENTITY_TOKEN_FILE = webIdentityTokenFile(AWS_CREDENTIALS_PROVIDER);

	/**
	 * The credentials provider that provides credentials for assuming the role when credential
	 * provider type is set to ASSUME_ROLE.
	 * Roles can be nested, so AWS_ROLE_CREDENTIALS_PROVIDER can again be set to "ASSUME_ROLE"
	 */
	public static final String AWS_ROLE_CREDENTIALS_PROVIDER = roleCredentialsProvider(AWS_CREDENTIALS_PROVIDER);

	/** The AWS endpoint for Kinesis (derived from the AWS region setting if not set). */
	public static final String AWS_ENDPOINT = "aws.endpoint";

	public static String accessKeyId(String prefix) {
		return prefix + ".basic.accesskeyid";
	}

	public static String secretKey(String prefix) {
		return prefix + ".basic.secretkey";
	}

	public static String profilePath(String prefix) {
		return prefix + ".profile.path";
	}

	public static String profileName(String prefix) {
		return prefix + ".profile.name";
	}

	public static String roleArn(String prefix) {
		return prefix + ".role.arn";
	}

	public static String roleSessionName(String prefix) {
		return prefix + ".role.sessionName";
	}

	public static String externalId(String prefix) {
		return prefix + ".role.externalId";
	}

	public static String roleCredentialsProvider(String prefix) {
		return prefix + ".role.provider";
	}

	public static String webIdentityTokenFile(String prefix) {
		return prefix + ".webIdentityToken.file";
	}
}
