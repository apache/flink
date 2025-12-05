/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.fs.s3.common.token;

import org.apache.flink.annotation.Internal;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.core.exception.SdkException;

import java.net.URI;

/**
 * Support dynamic session credentials for authenticating with AWS using SDK v2. Please note that
 * users may reference this class name from configuration property fs.s3a.aws.credentials.provider.
 * Therefore, changing the class name would be a backward-incompatible change. This credential
 * provider must not fail in creation because that will break a chain of credential providers.
 */
@Internal
public class HadoopDynamicTemporaryAWSCredentialsProvider implements AwsCredentialsProvider {

    public static final String NAME = HadoopDynamicTemporaryAWSCredentialsProvider.class.getName();

    public static final String COMPONENT = "Dynamic session credentials for Flink (SDK v2)";

    private static final Logger LOG =
            LoggerFactory.getLogger(HadoopDynamicTemporaryAWSCredentialsProvider.class);

    public HadoopDynamicTemporaryAWSCredentialsProvider() {}

    public HadoopDynamicTemporaryAWSCredentialsProvider(URI uri, Configuration conf) {}

    @Override
    public AwsCredentials resolveCredentials() throws SdkException {
        software.amazon.awssdk.services.sts.model.Credentials credentials =
                HadoopS3DelegationTokenReceiver.getCredentials();
        if (credentials == null) {
            throw SdkException.create(
                    COMPONENT + ": No delegation token credentials available", null);
        }
        LOG.debug("Providing session credentials");
        return AwsSessionCredentials.create(
                credentials.accessKeyId(),
                credentials.secretAccessKey(),
                credentials.sessionToken());
    }
}
