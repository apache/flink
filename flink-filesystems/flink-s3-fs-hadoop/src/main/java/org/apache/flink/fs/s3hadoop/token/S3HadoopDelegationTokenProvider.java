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

package org.apache.flink.fs.s3hadoop.token;

import org.apache.flink.annotation.Internal;
import org.apache.flink.fs.s3.common.token.AbstractS3DelegationTokenProvider;
import org.apache.flink.fs.s3.common.token.S3SessionCredentials;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.Credentials;

/** Delegation token provider for S3 Hadoop filesystems, based on AWS SDK v2. */
@Internal
public class S3HadoopDelegationTokenProvider extends AbstractS3DelegationTokenProvider {

    @Override
    public String serviceName() {
        return "s3-hadoop";
    }

    @Override
    protected S3SessionCredentials getSessionCredentials(
            String region, String accessKey, String secretKey) {
        try (StsClient stsClient =
                StsClient.builder()
                        .region(Region.of(region))
                        .credentialsProvider(
                                StaticCredentialsProvider.create(
                                        AwsBasicCredentials.create(accessKey, secretKey)))
                        .build()) {
            Credentials credentials = stsClient.getSessionToken().credentials();
            return new S3SessionCredentials(
                    credentials.accessKeyId(),
                    credentials.secretAccessKey(),
                    credentials.sessionToken(),
                    credentials.expiration().toEpochMilli());
        }
    }
}
