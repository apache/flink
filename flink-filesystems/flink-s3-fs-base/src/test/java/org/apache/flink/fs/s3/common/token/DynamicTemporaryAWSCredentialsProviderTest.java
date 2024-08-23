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

import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.securitytoken.model.Credentials;
import org.apache.hadoop.fs.s3a.auth.NoAwsCredentialsException;
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

    @BeforeEach
    void beforeEach() {
        AbstractS3DelegationTokenReceiver.credentials = null;
    }

    @AfterEach
    void afterEach() {
        AbstractS3DelegationTokenReceiver.credentials = null;
    }

    @Test
    void getCredentialsShouldThrowExceptionWhenNoCredentials() {
        DynamicTemporaryAWSCredentialsProvider provider =
                new DynamicTemporaryAWSCredentialsProvider();

        assertThatThrownBy(provider::getCredentials).isInstanceOf(NoAwsCredentialsException.class);
    }

    @Test
    void getCredentialsShouldStoreCredentialsWhenCredentialsProvided() throws Exception {
        DynamicTemporaryAWSCredentialsProvider provider =
                new DynamicTemporaryAWSCredentialsProvider();
        Credentials credentials =
                new Credentials(ACCESS_KEY_ID, SECRET_ACCESS_KEY, SESSION_TOKEN, null);
        AbstractS3DelegationTokenReceiver receiver =
                new AbstractS3DelegationTokenReceiver() {
                    @Override
                    public String serviceName() {
                        return "s3";
                    }
                };

        receiver.onNewTokensObtained(InstantiationUtil.serializeObject(credentials));
        BasicSessionCredentials returnedCredentials =
                (BasicSessionCredentials) provider.getCredentials();
        assertThat(returnedCredentials.getAWSAccessKeyId()).isEqualTo(credentials.getAccessKeyId());
        assertThat(returnedCredentials.getAWSSecretKey())
                .isEqualTo(credentials.getSecretAccessKey());
        assertThat(returnedCredentials.getSessionToken()).isEqualTo(credentials.getSessionToken());
    }
}
