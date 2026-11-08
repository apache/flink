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

import org.apache.flink.util.InstantiationUtil;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DynamicTemporaryAWSCredentialsProvider}. */
class DynamicTemporaryAWSCredentialsProviderTest {

    private static final String ACCESS_KEY_ID = "testAccessKeyId";
    private static final String SECRET_ACCESS_KEY = "testSecretAccessKey";
    private static final String SESSION_TOKEN = "testSessionToken";
    private static final long EXPIRATION_EPOCH_MILLI = 1234567890L;

    @BeforeEach
    void beforeEach() {
        AbstractS3DelegationTokenReceiver.credentials = null;
    }

    @AfterEach
    void afterEach() {
        AbstractS3DelegationTokenReceiver.credentials = null;
    }

    @Test
    void nameMustMatchClassName() {
        // Users reference this name from fs.s3a.aws.credentials.provider.
        assertThat(DynamicTemporaryAWSCredentialsProvider.NAME)
                .isEqualTo(DynamicTemporaryAWSCredentialsProvider.class.getName());
    }

    @Test
    void getCredentialsShouldThrowSdkV1ExceptionWhenNoCredentials() {
        DynamicTemporaryAWSCredentialsProvider provider =
                new DynamicTemporaryAWSCredentialsProvider();

        // Must be the SDK v1 exception: Hadoop's NoAwsCredentialsException is based on the AWS SDK
        // v2 exception hierarchy since Hadoop 3.4 and cannot be loaded in the presto plugin.
        assertThatThrownBy(provider::getCredentials).isInstanceOf(SdkClientException.class);
    }

    @Test
    void getCredentialsShouldReturnSessionCredentialsWhenProvided() throws Exception {
        DynamicTemporaryAWSCredentialsProvider provider =
                new DynamicTemporaryAWSCredentialsProvider();
        S3SessionCredentials credentials =
                new S3SessionCredentials(
                        ACCESS_KEY_ID, SECRET_ACCESS_KEY, SESSION_TOKEN, EXPIRATION_EPOCH_MILLI);
        AbstractS3DelegationTokenReceiver receiver =
                new AbstractS3DelegationTokenReceiver() {
                    @Override
                    public String serviceName() {
                        return "s3";
                    }
                };

        receiver.onNewTokensObtained(InstantiationUtil.serializeObject(credentials));

        AWSCredentials v1Credentials = provider.getCredentials();
        assertThat(v1Credentials).isInstanceOf(BasicSessionCredentials.class);
        BasicSessionCredentials sessionCredentials = (BasicSessionCredentials) v1Credentials;
        assertThat(sessionCredentials.getAWSAccessKeyId()).isEqualTo(ACCESS_KEY_ID);
        assertThat(sessionCredentials.getAWSSecretKey()).isEqualTo(SECRET_ACCESS_KEY);
        assertThat(sessionCredentials.getSessionToken()).isEqualTo(SESSION_TOKEN);
    }
}
