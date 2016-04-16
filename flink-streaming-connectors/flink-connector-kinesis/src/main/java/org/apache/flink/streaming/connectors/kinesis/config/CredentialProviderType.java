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

import com.amazonaws.auth.AWSCredentialsProvider;

/**
 * Possible configuration values for the type of credential provider to use when accessing AWS Kinesis.
 * Internally, a corresponding implementation of {@link AWSCredentialsProvider} will be used.
 */
public enum CredentialProviderType {

	/** Look for the environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_KEY to create AWS credentials */
	ENV_VAR,

	/** Look for Java system properties aws.accessKeyId and aws.secretKey to create AWS credentials */
	SYS_PROP,

	/** Use a AWS credentials profile file to create the AWS credentials */
	PROFILE,

	/** Simply create AWS credentials by supplying the AWS access key ID and AWS secret key in the configuration properties */
	BASIC
}
