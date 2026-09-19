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

package org.apache.flink.fs.s3presto.token;

import org.apache.flink.annotation.Internal;
import org.apache.flink.fs.s3.common.token.AbstractS3DelegationTokenProvider;
import org.apache.flink.fs.s3.common.token.S3SessionCredentials;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.Credentials;

/** Delegation token provider for S3 Presto filesystems, based on AWS SDK v1. */
@Internal
public class S3PrestoDelegationTokenProvider extends AbstractS3DelegationTokenProvider {

    @Override
    public String serviceName() {
        return "s3-presto";
    }

    @Override
    protected S3SessionCredentials getSessionCredentials(
            String region, String accessKey, String secretKey) {
        AWSSecurityTokenService stsClient =
                AWSSecurityTokenServiceClientBuilder.standard()
                        .withRegion(region)
                        .withCredentials(
                                new AWSStaticCredentialsProvider(
                                        new BasicAWSCredentials(accessKey, secretKey)))
                        .build();
        try {
            Credentials credentials = stsClient.getSessionToken().getCredentials();
            return new S3SessionCredentials(
                    credentials.getAccessKeyId(),
                    credentials.getSecretAccessKey(),
                    credentials.getSessionToken(),
                    credentials.getExpiration().getTime());
        } finally {
            stsClient.shutdown();
        }
    }
}
