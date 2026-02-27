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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.auth.NoAwsCredentialsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.services.sts.model.Credentials;

import java.net.URI;

/**
 * Support dynamic session credentials for authenticating with AWS. Please note that users may
 * reference this class name from configuration property fs.s3a.aws.credentials.provider. Therefore,
 * changing the class name would be a backward-incompatible change. This credential provider must
 * not fail in creation because that will break a chain of credential providers.
 *
 * <p>This class implements both AWS SDK V1 {@link AWSCredentialsProvider} (for Presto) and AWS SDK
 * V2 {@link AwsCredentialsProvider} (for Hadoop 3.4.0+) interfaces to support both filesystems.
 */
@Internal
public class DynamicTemporaryAWSCredentialsProvider
        implements AWSCredentialsProvider, AwsCredentialsProvider {

    public static final String NAME = DynamicTemporaryAWSCredentialsProvider.class.getName();

    public static final String COMPONENT = "Dynamic session credentials for Flink";

    private static final Logger LOG =
            LoggerFactory.getLogger(DynamicTemporaryAWSCredentialsProvider.class);

    public DynamicTemporaryAWSCredentialsProvider() {}

    public DynamicTemporaryAWSCredentialsProvider(URI uri, Configuration conf) {}

    /**
     * AWS SDK V1 method - used by Presto S3 filesystem.
     *
     * @return AWS SDK V1 credentials
     */
    @Override
    public AWSCredentials getCredentials() {
        Credentials credentials = AbstractS3DelegationTokenReceiver.getCredentials();
        if (credentials == null) {
            throw new NoAwsCredentialsException(COMPONENT);
        }
        LOG.debug("Providing session credentials (SDK V1)");
        return new BasicSessionCredentials(
                credentials.accessKeyId(),
                credentials.secretAccessKey(),
                credentials.sessionToken());
    }

    /** AWS SDK V1 method - refresh is a no-op as credentials are managed externally. */
    @Override
    public void refresh() {
        // Credentials are managed by the DelegationTokenReceiver, no-op here
    }

    /**
     * AWS SDK V2 method - used by Hadoop 3.4.0+ S3A filesystem.
     *
     * @return AWS SDK V2 credentials
     */
    @Override
    public software.amazon.awssdk.auth.credentials.AwsCredentials resolveCredentials() {
        Credentials credentials = AbstractS3DelegationTokenReceiver.getCredentials();
        if (credentials == null) {
            throw new NoAwsCredentialsException(COMPONENT);
        }
        LOG.debug("Providing session credentials (SDK V2)");
        return AwsSessionCredentials.create(
                credentials.accessKeyId(),
                credentials.secretAccessKey(),
                credentials.sessionToken());
    }
}
