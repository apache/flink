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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link HadoopDynamicTemporaryAWSCredentialsProvider}. */
class HadoopDynamicTemporaryAWSCredentialsProviderTest {

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
        // NAME is a string literal so that referencing it (e.g. when registering the provider in
        // the Hadoop configuration) never class-loads this provider outside this plugin; this pins
        // the literal to the actual class name, which users reference from
        // fs.s3a.aws.credentials.provider.
        assertThat(HadoopDynamicTemporaryAWSCredentialsProvider.NAME)
                .isEqualTo(HadoopDynamicTemporaryAWSCredentialsProvider.class.getName());
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
        S3SessionCredentials credentials =
                new S3SessionCredentials(
                        ACCESS_KEY_ID, SECRET_ACCESS_KEY, SESSION_TOKEN, EXPIRATION_EPOCH_MILLI);
        AbstractS3DelegationTokenReceiver receiver =
                new AbstractS3DelegationTokenReceiver() {
                    @Override
                    public String serviceName() {
                        return "s3-hadoop";
                    }
                };

        receiver.onNewTokensObtained(InstantiationUtil.serializeObject(credentials));

        AwsCredentials resolved = provider.resolveCredentials();
        assertThat(resolved).isInstanceOf(AwsSessionCredentials.class);
        AwsSessionCredentials sessionCredentials = (AwsSessionCredentials) resolved;
        assertThat(sessionCredentials.accessKeyId()).isEqualTo(ACCESS_KEY_ID);
        assertThat(sessionCredentials.secretAccessKey()).isEqualTo(SECRET_ACCESS_KEY);
        assertThat(sessionCredentials.sessionToken()).isEqualTo(SESSION_TOKEN);
    }
}
