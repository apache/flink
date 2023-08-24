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

package org.apache.flink.runtime.security.token.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Clock;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import static java.time.Instant.ofEpochMilli;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link HadoopFSDelegationTokenProvider}. */
class HadoopFSDelegationTokenProviderITCase {

    private static final long NOW = 100;

    private static final String masterPrincipal = "MasterPrincipal";

    final Text tokenService1 = new Text("TEST_TOKEN_SERVICE1");
    final Text tokenService2 = new Text("TEST_TOKEN_SERVICE2");

    private class TestDelegationToken extends Token<TestHadoopDelegationTokenIdentifier> {

        private long newExpiration;

        public TestDelegationToken(
                Text tokenService,
                TestHadoopDelegationTokenIdentifier identifier,
                long newExpiration) {
            super(identifier.getBytes(), new byte[4], identifier.getKind(), tokenService);
            this.newExpiration = newExpiration;
        }

        public TestDelegationToken(
                Text tokenService, TestHadoopDelegationTokenIdentifier identifier) {
            this(tokenService, identifier, 0L);
        }

        @Override
        public long renew(Configuration conf) {
            return newExpiration;
        }
    }

    @Test
    void getRenewerShouldReturnNullByDefault() throws Exception {
        HadoopFSDelegationTokenProvider provider = new HadoopFSDelegationTokenProvider();
        provider.init(new org.apache.flink.configuration.Configuration());
        assertThat(provider.getRenewer()).isNull();
    }

    @Test
    void getRenewerShouldReturnConfiguredRenewer() throws Exception {
        String renewer = "testRenewer";
        HadoopFSDelegationTokenProvider provider = new HadoopFSDelegationTokenProvider();
        org.apache.flink.configuration.Configuration configuration =
                new org.apache.flink.configuration.Configuration();
        configuration.setString("security.kerberos.token.provider.hadoopfs.renewer", renewer);
        provider.init(configuration);
        assertThat(provider.getRenewer()).isEqualTo(renewer);
    }

    @Test
    void getTokenRenewalIntervalShouldReturnNoneWhenNoTokens() throws IOException {
        HadoopFSDelegationTokenProvider provider =
                new HadoopFSDelegationTokenProvider() {
                    @Override
                    protected void obtainDelegationTokens(
                            String renewer,
                            Set<FileSystem> fileSystemsToAccess,
                            Credentials credentials) {}
                };
        Clock constantClock = Clock.fixed(ofEpochMilli(0), ZoneId.systemDefault());

        assertThat(provider.getTokenRenewalInterval(constantClock, Collections.emptySet()))
                .isEmpty();
    }

    @Test
    void getTokenRenewalIntervalShouldReturnMinWhenMultipleTokens() throws IOException {
        Clock constantClock = Clock.fixed(ofEpochMilli(NOW), ZoneId.systemDefault());
        HadoopFSDelegationTokenProvider provider =
                new HadoopFSDelegationTokenProvider() {
                    @Override
                    protected void obtainDelegationTokens(
                            String renewer,
                            Set<FileSystem> fileSystemsToAccess,
                            Credentials credentials) {
                        TestHadoopDelegationTokenIdentifier tokenIdentifier1 =
                                new TestHadoopDelegationTokenIdentifier(NOW);
                        credentials.addToken(
                                tokenService1,
                                new TestDelegationToken(tokenService1, tokenIdentifier1, NOW + 1));

                        TestHadoopDelegationTokenIdentifier tokenIdentifier2 =
                                new TestHadoopDelegationTokenIdentifier(NOW);
                        credentials.addToken(
                                tokenService2,
                                new TestDelegationToken(tokenService2, tokenIdentifier2, NOW + 2));
                    }
                };

        assertThat(provider.getTokenRenewalInterval(constantClock, Collections.emptySet()))
                .isEqualTo(Optional.of(1L));
    }

    @Test
    void getTokenRenewalDateShouldReturnNoneWhenNegativeRenewalInterval() {
        HadoopFSDelegationTokenProvider provider = new HadoopFSDelegationTokenProvider();
        Clock constantClock = Clock.fixed(ofEpochMilli(0), ZoneId.systemDefault());
        Credentials credentials = new Credentials();

        assertThat(provider.getTokenRenewalDate(constantClock, credentials, -1)).isEmpty();
    }

    @Test
    void getTokenRenewalDateShouldReturnNoneWhenNoTokens() {
        HadoopFSDelegationTokenProvider provider = new HadoopFSDelegationTokenProvider();
        Clock constantClock = Clock.fixed(ofEpochMilli(0), ZoneId.systemDefault());
        Credentials credentials = new Credentials();

        assertThat(provider.getTokenRenewalDate(constantClock, credentials, 1)).isEmpty();
    }

    @Test
    void getTokenRenewalDateShouldReturnMinWhenMultipleTokens() {
        HadoopFSDelegationTokenProvider provider = new HadoopFSDelegationTokenProvider();
        Clock constantClock = Clock.fixed(ofEpochMilli(NOW), ZoneId.systemDefault());
        Credentials credentials = new Credentials();
        TestHadoopDelegationTokenIdentifier tokenIdentifier1 =
                new TestHadoopDelegationTokenIdentifier(NOW);
        credentials.addToken(
                tokenService1, new TestDelegationToken(tokenService1, tokenIdentifier1));
        TestHadoopDelegationTokenIdentifier tokenIdentifier2 =
                new TestHadoopDelegationTokenIdentifier(NOW + 1);
        credentials.addToken(
                tokenService2, new TestDelegationToken(tokenService2, tokenIdentifier2));

        assertThat(provider.getTokenRenewalDate(constantClock, credentials, 1))
                .isEqualTo(Optional.of(NOW + 1));
    }

    @Test
    void getIssueDateShouldReturnIssueDateWithFutureToken() {
        HadoopFSDelegationTokenProvider provider = new HadoopFSDelegationTokenProvider();

        Clock constantClock = Clock.fixed(ofEpochMilli(NOW), ZoneId.systemDefault());
        long issueDate = NOW + 1;
        AbstractDelegationTokenIdentifier tokenIdentifier =
                new TestHadoopDelegationTokenIdentifier(issueDate);

        assertThat(
                        provider.getIssueDate(
                                constantClock,
                                tokenIdentifier.getKind().toString(),
                                tokenIdentifier))
                .isEqualTo(issueDate);
    }

    @Test
    void getIssueDateShouldReturnIssueDateWithPastToken() {
        HadoopFSDelegationTokenProvider provider = new HadoopFSDelegationTokenProvider();

        Clock constantClock = Clock.fixed(ofEpochMilli(NOW), ZoneId.systemDefault());
        long issueDate = NOW - 1;
        AbstractDelegationTokenIdentifier tokenIdentifier =
                new TestHadoopDelegationTokenIdentifier(issueDate);

        assertThat(
                        provider.getIssueDate(
                                constantClock,
                                tokenIdentifier.getKind().toString(),
                                tokenIdentifier))
                .isEqualTo(issueDate);
    }

    @Test
    void getIssueDateShouldReturnNowWithInvalidToken() {
        HadoopFSDelegationTokenProvider provider = new HadoopFSDelegationTokenProvider();

        Clock constantClock = Clock.fixed(ofEpochMilli(NOW), ZoneId.systemDefault());
        long issueDate = -1;
        AbstractDelegationTokenIdentifier tokenIdentifier =
                new TestHadoopDelegationTokenIdentifier(issueDate);

        assertThat(
                        provider.getIssueDate(
                                constantClock,
                                tokenIdentifier.getKind().toString(),
                                tokenIdentifier))
                .isEqualTo(NOW);
    }
}
