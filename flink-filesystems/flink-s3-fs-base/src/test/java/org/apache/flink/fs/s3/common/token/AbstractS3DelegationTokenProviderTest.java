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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.security.token.DelegationTokenProvider.ObtainedDelegationTokens;
import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.flink.core.security.token.DelegationTokenProvider.CONFIG_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AbstractS3DelegationTokenProvider}. */
class AbstractS3DelegationTokenProviderTest {

    private static final String REGION = "testRegion";
    private static final String ACCESS_KEY_ID = "testAccessKeyId";
    private static final String SECRET_ACCESS_KEY = "testSecretAccessKey";

    private final TestingSessionTokenClient client = new TestingSessionTokenClient();

    private String seenRegion;
    private String seenAccessKey;
    private String seenSecretKey;

    private AbstractS3DelegationTokenProvider provider;

    @BeforeEach
    void beforeEach() {
        provider =
                new AbstractS3DelegationTokenProvider() {
                    @Override
                    public String serviceName() {
                        return "s3";
                    }

                    @Override
                    protected SessionTokenClient createSessionTokenClient(
                            String region, String accessKey, String secretKey) {
                        seenRegion = region;
                        seenAccessKey = accessKey;
                        seenSecretKey = secretKey;
                        return client;
                    }
                };
    }

    @Test
    void delegationTokensRequiredShouldReturnFalseWithoutCredentials() {
        provider.init(new Configuration());
        assertThat(provider.delegationTokensRequired()).isFalse();
    }

    @Test
    void delegationTokensRequiredShouldReturnTrueWithCredentials() {
        provider.init(createConfiguration());

        assertThat(provider.delegationTokensRequired()).isTrue();
    }

    @Test
    void obtainDelegationTokensShouldUseConfiguredCredentials() throws Exception {
        provider.init(createConfiguration());

        provider.obtainDelegationTokens();

        assertThat(seenRegion).isEqualTo(REGION);
        assertThat(seenAccessKey).isEqualTo(ACCESS_KEY_ID);
        assertThat(seenSecretKey).isEqualTo(SECRET_ACCESS_KEY);
    }

    @Test
    void obtainDelegationTokensShouldShutdownClientAfterSuccess() throws Exception {
        final ObtainedDelegationTokens tokens = provider.obtainDelegationTokens();

        assertTokensContain(tokens, client.credentials);
        assertThat(client.requestCount).isEqualTo(1);
        assertThat(client.closeCount).isEqualTo(1);
    }

    @Test
    void obtainDelegationTokensShouldShutdownClientAfterRequestFailure() {
        final RuntimeException requestFailure = new RuntimeException("STS request failed");
        client.requestFailure = requestFailure;

        assertThatThrownBy(provider::obtainDelegationTokens).isSameAs(requestFailure);

        assertThat(client.closeCount).isEqualTo(1);
    }

    @Test
    void obtainDelegationTokensShouldPreserveRequestFailureWhenShutdownFails() {
        final RuntimeException requestFailure = new RuntimeException("STS request failed");
        client.requestFailure = requestFailure;
        client.closeFailure = new RuntimeException("STS shutdown failed");

        assertThatThrownBy(provider::obtainDelegationTokens).isSameAs(requestFailure);
        assertThat(client.closeCount).isEqualTo(1);
    }

    @Test
    void obtainDelegationTokensShouldReturnTokensWhenShutdownFails() throws Exception {
        client.closeFailure = new RuntimeException("STS shutdown failed");

        final ObtainedDelegationTokens tokens = provider.obtainDelegationTokens();

        assertTokensContain(tokens, client.credentials);
        assertThat(client.requestCount).isEqualTo(1);
        assertThat(client.closeCount).isEqualTo(1);
    }

    private static Configuration createConfiguration() {
        final Configuration configuration = new Configuration();
        configuration.setString(CONFIG_PREFIX + ".s3.region", REGION);
        configuration.setString(CONFIG_PREFIX + ".s3.access-key", ACCESS_KEY_ID);
        configuration.setString(CONFIG_PREFIX + ".s3.secret-key", SECRET_ACCESS_KEY);
        return configuration;
    }

    private static void assertTokensContain(
            ObtainedDelegationTokens tokens, S3SessionCredentials credentials) throws Exception {
        final S3SessionCredentials deserializedCredentials =
                InstantiationUtil.deserializeObject(
                        tokens.getTokens(),
                        AbstractS3DelegationTokenProviderTest.class.getClassLoader());
        assertThat(deserializedCredentials.getAccessKeyId())
                .isEqualTo(credentials.getAccessKeyId());
        assertThat(deserializedCredentials.getSecretAccessKey())
                .isEqualTo(credentials.getSecretAccessKey());
        assertThat(deserializedCredentials.getSessionToken())
                .isEqualTo(credentials.getSessionToken());
        assertThat(deserializedCredentials.getExpirationEpochMilli())
                .isEqualTo(credentials.getExpirationEpochMilli());
        assertThat(tokens.getValidUntil()).contains(credentials.getExpirationEpochMilli());
    }

    private static class TestingSessionTokenClient
            implements AbstractS3DelegationTokenProvider.SessionTokenClient {
        private final S3SessionCredentials credentials =
                new S3SessionCredentials(
                        ACCESS_KEY_ID, SECRET_ACCESS_KEY, "testSessionToken", 123456789L);
        private RuntimeException requestFailure;
        private RuntimeException closeFailure;
        private int requestCount;
        private int closeCount;

        @Override
        public S3SessionCredentials getSessionCredentials() {
            if (closeCount > 0) {
                throw new IllegalStateException("STS client already shut down");
            }
            requestCount++;
            if (requestFailure != null) {
                throw requestFailure;
            }
            return credentials;
        }

        @Override
        public void close() {
            closeCount++;
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }
}
