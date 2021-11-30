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

/** Configuration keys for AWS service usage. */
@PublicEvolving
public class AWSConfigConstants extends org.apache.flink.connector.aws.config.AWSConfigConstants {
    /**
     * Possible configuration values for the type of credential provider to use when accessing AWS
     * Kinesis. Internally, a corresponding implementation of {@link AwsCredentialsProvider} will be
     * used. Copy of org.apache.flink.connector.aws.config.AWSConfigConstants.CredentialProvider
     * left for backwards compatibility
     */
    public enum CredentialProvider {

        /**
         * Look for the environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY to create
         * AWS credentials.
         */
        ENV_VAR,

        /**
         * Look for Java system properties aws.accessKeyId and aws.secretKey to create AWS
         * credentials.
         */
        SYS_PROP,

        /** Use a AWS credentials profile file to create the AWS credentials. */
        PROFILE,

        /**
         * Simply create AWS credentials by supplying the AWS access key ID and AWS secret key in
         * the configuration properties.
         */
        BASIC,

        /**
         * Create AWS credentials by assuming a role. The credentials for assuming the role must be
         * supplied. *
         */
        ASSUME_ROLE,

        /**
         * Use AWS WebIdentityToken in order to assume a role. A token file and role details can be
         * supplied as configuration or environment variables. *
         */
        WEB_IDENTITY_TOKEN,

        /**
         * A credentials provider chain will be used that searches for credentials in this order:
         * ENV_VARS, SYS_PROPS, WEB_IDENTITY_TOKEN, PROFILE in the AWS instance metadata. *
         */
        AUTO,
    }
}
