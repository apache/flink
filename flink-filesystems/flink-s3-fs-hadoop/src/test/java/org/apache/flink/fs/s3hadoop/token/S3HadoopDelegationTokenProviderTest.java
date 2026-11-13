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

package org.apache.flink.fs.s3hadoop.token;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.security.token.DelegationTokenProvider.ObtainedDelegationTokens;
import org.apache.flink.fs.s3.common.token.S3SessionCredentials;
import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.Credentials;
import software.amazon.awssdk.services.sts.model.GetSessionTokenRequest;
import software.amazon.awssdk.services.sts.model.GetSessionTokenResponse;

import java.time.Instant;

import static org.apache.flink.core.security.token.DelegationTokenProvider.CONFIG_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link S3HadoopDelegationTokenProvider}. */
class S3HadoopDelegationTokenProviderTest {

    private final TestingStsClient stsClient = new TestingStsClient();

    private S3HadoopDelegationTokenProvider provider;

    @BeforeEach
    void beforeEach() {
        provider =
                new S3HadoopDelegationTokenProvider() {
                    @Override
                    StsClient createStsClient(String region, String accessKey, String secretKey) {
                        return stsClient;
                    }
                };
        final Configuration configuration = new Configuration();
        configuration.setString(CONFIG_PREFIX + ".s3-hadoop.region", "testRegion");
        configuration.setString(CONFIG_PREFIX + ".s3-hadoop.access-key", "testAccessKeyId");
        configuration.setString(CONFIG_PREFIX + ".s3-hadoop.secret-key", "testSecretAccessKey");
        provider.init(configuration);
    }

    @Test
    void serviceNameIsS3Hadoop() {
        assertThat(new S3HadoopDelegationTokenProvider().serviceName()).isEqualTo("s3-hadoop");
    }

    @Test
    void serviceConfigPrefixIsDerivedFromConfigPrefixAndServiceName() {
        assertThat(new S3HadoopDelegationTokenProvider().serviceConfigPrefix())
                .isEqualTo("security.delegation.token.provider.s3-hadoop");
    }

    @Test
    void obtainDelegationTokensShouldConvertCredentialsAndCloseClient() throws Exception {
        final ObtainedDelegationTokens tokens = provider.obtainDelegationTokens();

        final S3SessionCredentials credentials =
                InstantiationUtil.deserializeObject(
                        tokens.getTokens(), getClass().getClassLoader());
        assertThat(credentials.getAccessKeyId()).isEqualTo("testAccessKeyId");
        assertThat(credentials.getSecretAccessKey()).isEqualTo("testSecretAccessKey");
        assertThat(credentials.getSessionToken()).isEqualTo("testSessionToken");
        assertThat(credentials.getExpirationEpochMilli()).isEqualTo(123456789L);
        assertThat(tokens.getValidUntil()).contains(123456789L);
        assertThat(stsClient.closeCount).isEqualTo(1);
    }

    @Test
    void obtainDelegationTokensShouldCloseClientAfterConversionFailure() {
        stsClient.expiration = null;

        assertThatThrownBy(provider::obtainDelegationTokens)
                .isInstanceOf(NullPointerException.class);

        assertThat(stsClient.closeCount).isEqualTo(1);
    }

    private static class TestingStsClient implements StsClient {
        private Instant expiration = Instant.ofEpochMilli(123456789L);
        private int closeCount;

        @Override
        public GetSessionTokenResponse getSessionToken(GetSessionTokenRequest request) {
            return GetSessionTokenResponse.builder()
                    .credentials(
                            Credentials.builder()
                                    .accessKeyId("testAccessKeyId")
                                    .secretAccessKey("testSecretAccessKey")
                                    .sessionToken("testSessionToken")
                                    .expiration(expiration)
                                    .build())
                    .build();
        }

        @Override
        public String serviceName() {
            return SERVICE_NAME;
        }

        @Override
        public void close() {
            closeCount++;
        }
    }
}
