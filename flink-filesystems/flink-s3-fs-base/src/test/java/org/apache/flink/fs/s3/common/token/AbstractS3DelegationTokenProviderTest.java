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

import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AbstractAWSSecurityTokenService;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.amazonaws.services.securitytoken.model.GetSessionTokenResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Date;

import static org.apache.flink.core.security.token.DelegationTokenProvider.CONFIG_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AbstractS3DelegationTokenProvider}. */
class AbstractS3DelegationTokenProviderTest {

    private static final String REGION = "testRegion";
    private static final String ACCESS_KEY_ID = "testAccessKeyId";
    private static final String SECRET_ACCESS_KEY = "testSecretAccessKey";

    private final TestingStsClient stsClient = new TestingStsClient();

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
                    AWSSecurityTokenService createStsClient() {
                        return stsClient;
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
        Configuration configuration = new Configuration();
        configuration.setString(CONFIG_PREFIX + ".s3.region", REGION);
        configuration.setString(CONFIG_PREFIX + ".s3.access-key", ACCESS_KEY_ID);
        configuration.setString(CONFIG_PREFIX + ".s3.secret-key", SECRET_ACCESS_KEY);
        provider.init(configuration);

        assertThat(provider.delegationTokensRequired()).isTrue();
    }

    @Test
    void obtainDelegationTokensShouldShutdownClientAfterSuccess() throws Exception {
        final Credentials credentials = createCredentials();
        stsClient.credentials = credentials;

        final ObtainedDelegationTokens tokens = provider.obtainDelegationTokens();

        final Credentials deserializedCredentials =
                InstantiationUtil.deserializeObject(
                        tokens.getTokens(), getClass().getClassLoader());
        assertThat(deserializedCredentials).isEqualTo(credentials);
        assertThat(tokens.getValidUntil()).contains(credentials.getExpiration().getTime());
        assertThat(stsClient.requestCount).isEqualTo(1);
        assertThat(stsClient.shutdownCount).isEqualTo(1);
    }

    @Test
    void obtainDelegationTokensShouldShutdownClientAfterRequestFailure() {
        final RuntimeException requestFailure = new RuntimeException("STS request failed");
        stsClient.requestFailure = requestFailure;

        assertThatThrownBy(provider::obtainDelegationTokens).isSameAs(requestFailure);

        assertThat(stsClient.shutdownCount).isEqualTo(1);
    }

    @Test
    void obtainDelegationTokensShouldShutdownClientAfterPostprocessingFailure() {
        stsClient.credentials = createCredentials().withExpiration(null);

        assertThatThrownBy(provider::obtainDelegationTokens)
                .isInstanceOf(NullPointerException.class);

        assertThat(stsClient.shutdownCount).isEqualTo(1);
    }

    @Test
    void obtainDelegationTokensShouldPreserveRequestFailureWhenShutdownFails() {
        final RuntimeException requestFailure = new RuntimeException("STS request failed");
        final RuntimeException shutdownFailure = new RuntimeException("STS shutdown failed");
        stsClient.requestFailure = requestFailure;
        stsClient.shutdownFailure = shutdownFailure;

        assertThatThrownBy(provider::obtainDelegationTokens).isSameAs(requestFailure);
        assertThat(requestFailure.getSuppressed()).containsExactly(shutdownFailure);
        assertThat(stsClient.shutdownCount).isEqualTo(1);
    }

    @Test
    void obtainDelegationTokensShouldPropagateShutdownFailure() {
        final RuntimeException shutdownFailure = new RuntimeException("STS shutdown failed");
        stsClient.shutdownFailure = shutdownFailure;

        assertThatThrownBy(provider::obtainDelegationTokens).isSameAs(shutdownFailure);

        assertThat(stsClient.requestCount).isEqualTo(1);
        assertThat(stsClient.shutdownCount).isEqualTo(1);
    }

    private static Credentials createCredentials() {
        return new Credentials()
                .withAccessKeyId(ACCESS_KEY_ID)
                .withSecretAccessKey(SECRET_ACCESS_KEY)
                .withSessionToken("testSessionToken")
                .withExpiration(new Date(123456789L));
    }

    private static class TestingStsClient extends AbstractAWSSecurityTokenService {
        private Credentials credentials = createCredentials();
        private RuntimeException requestFailure;
        private RuntimeException shutdownFailure;
        private int requestCount;
        private int shutdownCount;

        @Override
        public GetSessionTokenResult getSessionToken() {
            if (shutdownCount > 0) {
                throw new IllegalStateException("STS client already shut down");
            }
            requestCount++;
            if (requestFailure != null) {
                throw requestFailure;
            }
            return new GetSessionTokenResult().withCredentials(credentials);
        }

        @Override
        public void shutdown() {
            shutdownCount++;
            if (shutdownFailure != null) {
                throw shutdownFailure;
            }
        }
    }
}
