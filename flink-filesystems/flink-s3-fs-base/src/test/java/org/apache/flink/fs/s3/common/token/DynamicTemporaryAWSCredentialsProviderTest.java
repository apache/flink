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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import org.apache.hadoop.fs.s3a.auth.NoAwsCredentialsException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.services.sts.model.Credentials;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DynamicTemporaryAWSCredentialsProvider}. */
class DynamicTemporaryAWSCredentialsProviderTest {

    private static final String ACCESS_KEY_ID = "testAccessKeyId";
    private static final String SECRET_ACCESS_KEY = "testSecretAccessKey";
    private static final String SESSION_TOKEN = "testSessionToken";

    @BeforeEach
    void beforeEach() {
        AbstractS3DelegationTokenReceiver.credentials = null;
    }

    @AfterEach
    void afterEach() {
        AbstractS3DelegationTokenReceiver.credentials = null;
    }

    @Test
    void providerImplementsBothSdkInterfaces() {
        DynamicTemporaryAWSCredentialsProvider provider =
                new DynamicTemporaryAWSCredentialsProvider();

        // SDK V1 interface (for Presto)
        assertThat(provider).isInstanceOf(AWSCredentialsProvider.class);
        // SDK V2 interface (for Hadoop 3.4.0+)
        assertThat(provider).isInstanceOf(AwsCredentialsProvider.class);
    }

    @Test
    void getCredentialsShouldThrowExceptionWhenNoCredentials() {
        DynamicTemporaryAWSCredentialsProvider provider =
                new DynamicTemporaryAWSCredentialsProvider();

        // SDK V1 method
        assertThatThrownBy(provider::getCredentials).isInstanceOf(NoAwsCredentialsException.class);
        // SDK V2 method
        assertThatThrownBy(provider::resolveCredentials)
                .isInstanceOf(NoAwsCredentialsException.class);
    }

    @Test
    void getCredentialsShouldReturnSdkV1CredentialsWhenCredentialsProvided() throws Exception {
        DynamicTemporaryAWSCredentialsProvider provider =
                new DynamicTemporaryAWSCredentialsProvider();
        Credentials credentials =
                Credentials.builder()
                        .accessKeyId(ACCESS_KEY_ID)
                        .secretAccessKey(SECRET_ACCESS_KEY)
                        .sessionToken(SESSION_TOKEN)
                        .build();
        AbstractS3DelegationTokenReceiver receiver =
                new AbstractS3DelegationTokenReceiver() {
                    @Override
                    public String serviceName() {
                        return "s3";
                    }
                };

        receiver.onNewTokensObtained(InstantiationUtil.serializeObject(credentials));

        // Test SDK V1 getCredentials() method (used by Presto)
        AWSCredentials v1Credentials = provider.getCredentials();
        assertThat(v1Credentials).isInstanceOf(BasicSessionCredentials.class);
        BasicSessionCredentials sessionCredentials = (BasicSessionCredentials) v1Credentials;
        assertThat(sessionCredentials.getAWSAccessKeyId()).isEqualTo(credentials.accessKeyId());
        assertThat(sessionCredentials.getAWSSecretKey()).isEqualTo(credentials.secretAccessKey());
        assertThat(sessionCredentials.getSessionToken()).isEqualTo(credentials.sessionToken());
    }

    @Test
    void resolveCredentialsShouldReturnSdkV2CredentialsWhenCredentialsProvided() throws Exception {
        DynamicTemporaryAWSCredentialsProvider provider =
                new DynamicTemporaryAWSCredentialsProvider();
        Credentials credentials =
                Credentials.builder()
                        .accessKeyId(ACCESS_KEY_ID)
                        .secretAccessKey(SECRET_ACCESS_KEY)
                        .sessionToken(SESSION_TOKEN)
                        .build();
        AbstractS3DelegationTokenReceiver receiver =
                new AbstractS3DelegationTokenReceiver() {
                    @Override
                    public String serviceName() {
                        return "s3";
                    }
                };

        receiver.onNewTokensObtained(InstantiationUtil.serializeObject(credentials));

        // Test SDK V2 resolveCredentials() method (used by Hadoop 3.4.0+)
        software.amazon.awssdk.auth.credentials.AwsCredentials v2Credentials =
                provider.resolveCredentials();
        assertThat(v2Credentials).isInstanceOf(AwsSessionCredentials.class);
        AwsSessionCredentials sessionCredentials = (AwsSessionCredentials) v2Credentials;
        assertThat(sessionCredentials.accessKeyId()).isEqualTo(credentials.accessKeyId());
        assertThat(sessionCredentials.secretAccessKey()).isEqualTo(credentials.secretAccessKey());
        assertThat(sessionCredentials.sessionToken()).isEqualTo(credentials.sessionToken());
    }
}
