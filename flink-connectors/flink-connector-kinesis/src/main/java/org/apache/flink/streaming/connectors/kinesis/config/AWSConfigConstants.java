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

		/** A credentials provider chain will be used that searches for credentials in this order: ENV_VARS, SYS_PROPS, PROFILE in the AWS instance metadata. **/
		AUTO,
	}

	/** The AWS region of the Kinesis streams to be pulled ("us-east-1" is used if not set). */
	public static final String AWS_REGION = "aws.region";

	/** The AWS access key ID to use when setting credentials provider type to BASIC. */
	public static final String AWS_ACCESS_KEY_ID = "aws.credentials.provider.basic.accesskeyid";

	/** The AWS secret key to use when setting credentials provider type to BASIC. */
	public static final String AWS_SECRET_ACCESS_KEY = "aws.credentials.provider.basic.secretkey";

	/** The credential provider type to use when AWS credentials are required (BASIC is used if not set). */
	public static final String AWS_CREDENTIALS_PROVIDER = "aws.credentials.provider";

	/** Optional configuration for profile path if credential provider type is set to be PROFILE. */
	public static final String AWS_PROFILE_PATH = "aws.credentials.provider.profile.path";

	/** Optional configuration for profile name if credential provider type is set to be PROFILE. */
	public static final String AWS_PROFILE_NAME = "aws.credentials.provider.profile.name";

	/** The AWS endpoint for Kinesis (derived from the AWS region setting if not set). */
	public static final String AWS_ENDPOINT = "aws.endpoint";

}
