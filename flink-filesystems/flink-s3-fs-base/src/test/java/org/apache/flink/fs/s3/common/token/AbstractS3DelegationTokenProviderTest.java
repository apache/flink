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

import java.util.Optional;

import static org.apache.flink.core.security.token.DelegationTokenProvider.CONFIG_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AbstractS3DelegationTokenProvider}. */
class AbstractS3DelegationTokenProviderTest {

    private static final String REGION = "testRegion";
    private static final String ACCESS_KEY_ID = "testAccessKeyId";
    private static final String SECRET_ACCESS_KEY = "testSecretAccessKey";
    private static final String SESSION_TOKEN = "testSessionToken";
    private static final long EXPIRATION_EPOCH_MILLI = 1234567890L;

    /** Records the arguments of the STS call and returns fixed session credentials. */
    private static class TestS3DelegationTokenProvider extends AbstractS3DelegationTokenProvider {

        private String seenRegion;
        private String seenAccessKey;
        private String seenSecretKey;

        @Override
        public String serviceName() {
            return "s3";
        }

        @Override
        protected S3SessionCredentials getSessionCredentials(
                String region, String accessKey, String secretKey) {
            this.seenRegion = region;
            this.seenAccessKey = accessKey;
            this.seenSecretKey = secretKey;
            return new S3SessionCredentials(
                    ACCESS_KEY_ID, SECRET_ACCESS_KEY, SESSION_TOKEN, EXPIRATION_EPOCH_MILLI);
        }
    }

    private TestS3DelegationTokenProvider provider;

    @BeforeEach
    void beforeEach() {
        provider = new TestS3DelegationTokenProvider();
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
    void obtainDelegationTokensShouldSerializeSessionCredentials() throws Exception {
        provider.init(createConfiguration());

        ObtainedDelegationTokens tokens = provider.obtainDelegationTokens();

        assertThat(provider.seenRegion).isEqualTo(REGION);
        assertThat(provider.seenAccessKey).isEqualTo(ACCESS_KEY_ID);
        assertThat(provider.seenSecretKey).isEqualTo(SECRET_ACCESS_KEY);
        assertThat(tokens.getValidUntil()).isEqualTo(Optional.of(EXPIRATION_EPOCH_MILLI));

        S3SessionCredentials credentials =
                InstantiationUtil.deserializeObject(
                        tokens.getTokens(), getClass().getClassLoader());
        assertThat(credentials.getAccessKeyId()).isEqualTo(ACCESS_KEY_ID);
        assertThat(credentials.getSecretAccessKey()).isEqualTo(SECRET_ACCESS_KEY);
        assertThat(credentials.getSessionToken()).isEqualTo(SESSION_TOKEN);
        assertThat(credentials.getExpirationEpochMilli()).isEqualTo(EXPIRATION_EPOCH_MILLI);
    }

    private static Configuration createConfiguration() {
        Configuration configuration = new Configuration();
        configuration.setString(CONFIG_PREFIX + ".s3.region", REGION);
        configuration.setString(CONFIG_PREFIX + ".s3.access-key", ACCESS_KEY_ID);
        configuration.setString(CONFIG_PREFIX + ".s3.secret-key", SECRET_ACCESS_KEY);
        return configuration;
    }
}
