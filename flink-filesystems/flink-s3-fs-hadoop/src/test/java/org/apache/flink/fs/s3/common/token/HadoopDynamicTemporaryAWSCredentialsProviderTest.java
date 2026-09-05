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

import org.apache.hadoop.fs.s3a.auth.NoAwsCredentialsException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.services.sts.model.Credentials;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link HadoopDynamicTemporaryAWSCredentialsProvider}. */
class HadoopDynamicTemporaryAWSCredentialsProviderTest {

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
    void resolveCredentialsShouldThrowNoAwsCredentialsExceptionWhenNoCredentials() {
        HadoopDynamicTemporaryAWSCredentialsProvider provider =
                new HadoopDynamicTemporaryAWSCredentialsProvider();

        assertThatThrownBy(provider::resolveCredentials)
                .isInstanceOf(NoAwsCredentialsException.class);
    }

    @Test
    void resolveCredentialsShouldReturnSessionCredentialsWhenProvided() throws Exception {
        HadoopDynamicTemporaryAWSCredentialsProvider provider =
                new HadoopDynamicTemporaryAWSCredentialsProvider();
        Credentials credentials =
                Credentials.builder()
                        .accessKeyId(ACCESS_KEY_ID)
                        .secretAccessKey(SECRET_ACCESS_KEY)
                        .sessionToken(SESSION_TOKEN)
                        .build();
        HadoopS3DelegationTokenReceiver receiver =
                new HadoopS3DelegationTokenReceiver() {
                    @Override
                    public String serviceName() {
                        return "s3-hadoop";
                    }
                };

        receiver.onNewTokensObtained(InstantiationUtil.serializeObject(credentials));

        AwsCredentials resolved = provider.resolveCredentials();
        assertThat(resolved).isInstanceOf(AwsSessionCredentials.class);
        AwsSessionCredentials sessionCredentials = (AwsSessionCredentials) resolved;
        assertThat(sessionCredentials.accessKeyId()).isEqualTo(credentials.accessKeyId());
        assertThat(sessionCredentials.secretAccessKey()).isEqualTo(credentials.secretAccessKey());
        assertThat(sessionCredentials.sessionToken()).isEqualTo(credentials.sessionToken());
    }

    @Test
    void onNewTokensObtainedShouldPopulateSdkAgnosticCredentialsStore() throws Exception {
        Credentials credentials =
                Credentials.builder()
                        .accessKeyId(ACCESS_KEY_ID)
                        .secretAccessKey(SECRET_ACCESS_KEY)
                        .sessionToken(SESSION_TOKEN)
                        .build();
        HadoopS3DelegationTokenReceiver receiver =
                new HadoopS3DelegationTokenReceiver() {
                    @Override
                    public String serviceName() {
                        return "s3-hadoop";
                    }
                };

        receiver.onNewTokensObtained(InstantiationUtil.serializeObject(credentials));

        // SDK-agnostic consumers (e.g. the s5cmd integration in FlinkS3FileSystem) read the
        // shared store on AbstractS3DelegationTokenReceiver; the SDK v2 receiver must fill it.
        S3SessionCredentials stored = AbstractS3DelegationTokenReceiver.getCredentials();
        assertThat(stored).isNotNull();
        assertThat(stored.getAccessKeyId()).isEqualTo(ACCESS_KEY_ID);
        assertThat(stored.getSecretAccessKey()).isEqualTo(SECRET_ACCESS_KEY);
        assertThat(stored.getSessionToken()).isEqualTo(SESSION_TOKEN);
    }
}
