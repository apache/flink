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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.security.token.DelegationTokenProvider.ObtainedDelegationTokens;
import org.apache.flink.fs.s3.common.token.S3SessionCredentials;
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

/** Tests for {@link S3PrestoDelegationTokenProvider}. */
class S3PrestoDelegationTokenProviderTest {

    private final TestingStsClient stsClient = new TestingStsClient();

    private S3PrestoDelegationTokenProvider provider;

    @BeforeEach
    void beforeEach() {
        provider =
                new S3PrestoDelegationTokenProvider() {
                    @Override
                    AWSSecurityTokenService createStsClient(
                            String region, String accessKey, String secretKey) {
                        return stsClient;
                    }
                };
        final Configuration configuration = new Configuration();
        configuration.setString(CONFIG_PREFIX + ".s3-presto.region", "testRegion");
        configuration.setString(CONFIG_PREFIX + ".s3-presto.access-key", "testAccessKeyId");
        configuration.setString(CONFIG_PREFIX + ".s3-presto.secret-key", "testSecretAccessKey");
        provider.init(configuration);
    }

    @Test
    void serviceNameIsS3Presto() {
        assertThat(new S3PrestoDelegationTokenProvider().serviceName()).isEqualTo("s3-presto");
    }

    @Test
    void serviceConfigPrefixIsDerivedFromConfigPrefixAndServiceName() {
        assertThat(new S3PrestoDelegationTokenProvider().serviceConfigPrefix())
                .isEqualTo("security.delegation.token.provider.s3-presto");
    }

    @Test
    void obtainDelegationTokensShouldConvertCredentialsAndShutdownClient() throws Exception {
        final ObtainedDelegationTokens tokens = provider.obtainDelegationTokens();

        final S3SessionCredentials credentials =
                InstantiationUtil.deserializeObject(
                        tokens.getTokens(), getClass().getClassLoader());
        assertThat(credentials.getAccessKeyId()).isEqualTo("testAccessKeyId");
        assertThat(credentials.getSecretAccessKey()).isEqualTo("testSecretAccessKey");
        assertThat(credentials.getSessionToken()).isEqualTo("testSessionToken");
        assertThat(credentials.getExpirationEpochMilli()).isEqualTo(123456789L);
        assertThat(tokens.getValidUntil()).contains(123456789L);
        assertThat(stsClient.shutdownCount).isEqualTo(1);
    }

    @Test
    void obtainDelegationTokensShouldShutdownClientAfterConversionFailure() {
        stsClient.credentials.setExpiration(null);

        assertThatThrownBy(provider::obtainDelegationTokens)
                .isInstanceOf(NullPointerException.class);

        assertThat(stsClient.shutdownCount).isEqualTo(1);
    }

    private static class TestingStsClient extends AbstractAWSSecurityTokenService {
        private final Credentials credentials =
                new Credentials()
                        .withAccessKeyId("testAccessKeyId")
                        .withSecretAccessKey("testSecretAccessKey")
                        .withSessionToken("testSessionToken")
                        .withExpiration(new Date(123456789L));
        private int shutdownCount;

        @Override
        public GetSessionTokenResult getSessionToken() {
            return new GetSessionTokenResult().withCredentials(credentials);
        }

        @Override
        public void shutdown() {
            shutdownCount++;
        }
    }
}
