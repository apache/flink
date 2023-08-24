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

package org.apache.flink.runtime.security.token;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.security.token.DelegationTokenProvider;
import org.apache.flink.core.security.token.DelegationTokenReceiver;
import org.apache.flink.core.testutils.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static java.time.Instant.ofEpochMilli;
import static org.apache.flink.configuration.SecurityOptions.DELEGATION_TOKENS_RENEWAL_TIME_RATIO;
import static org.apache.flink.core.security.token.DelegationTokenProvider.CONFIG_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link DelegationTokenManager}. */
class DefaultDelegationTokenManagerTest {

    @BeforeEach
    void beforeEach() {
        ExceptionThrowingDelegationTokenProvider.reset();
        ExceptionThrowingDelegationTokenReceiver.reset();
    }

    @AfterEach
    void afterEach() {
        ExceptionThrowingDelegationTokenProvider.reset();
        ExceptionThrowingDelegationTokenReceiver.reset();
    }

    @Test
    void isProviderEnabledMustGiveBackTrueByDefault() {
        Configuration configuration = new Configuration();

        assertThat(DefaultDelegationTokenManager.isProviderEnabled(configuration, "test")).isTrue();
    }

    @Test
    void isProviderEnabledMustGiveBackFalseWhenDisabled() {
        Configuration configuration = new Configuration();
        configuration.setBoolean(CONFIG_PREFIX + ".test.enabled", false);

        assertThat(DefaultDelegationTokenManager.isProviderEnabled(configuration, "test"))
                .isFalse();
    }

    @Test
    void configurationIsNullMustFailFast() {
        assertThatThrownBy(() -> new DefaultDelegationTokenManager(null, null, null, null))
                .isInstanceOf(Exception.class);
    }

    @Test
    void oneProviderThrowsExceptionMustFailFast() {
        assertThatThrownBy(
                        () -> {
                            ExceptionThrowingDelegationTokenProvider.throwInInit.set(true);
                            new DefaultDelegationTokenManager(
                                    new Configuration(), null, null, null);
                        })
                .isInstanceOf(Exception.class);
    }

    @Test
    void testAllProvidersLoaded() {
        Configuration configuration = new Configuration();
        configuration.setBoolean(CONFIG_PREFIX + ".throw.enabled", false);
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(configuration, null, null, null);

        assertThat(delegationTokenManager.delegationTokenProviders).hasSize(3);

        assertThat(delegationTokenManager.isProviderLoaded("hadoopfs")).isTrue();
        assertThat(delegationTokenManager.isReceiverLoaded("hadoopfs")).isTrue();

        assertThat(delegationTokenManager.isProviderLoaded("hbase")).isTrue();
        assertThat(delegationTokenManager.isReceiverLoaded("hbase")).isTrue();

        assertThat(delegationTokenManager.isProviderLoaded("test")).isTrue();
        assertThat(delegationTokenManager.isReceiverLoaded("test")).isTrue();

        assertThat(ExceptionThrowingDelegationTokenProvider.constructed.get()).isTrue();
        assertThat(ExceptionThrowingDelegationTokenReceiver.constructed.get()).isTrue();
        assertThat(delegationTokenManager.isProviderLoaded("throw")).isFalse();
        assertThat(delegationTokenManager.isReceiverLoaded("throw")).isFalse();
    }

    @Test
    void checkProviderAndReceiverConsistencyShouldNotThrowWhenNothingLoaded() {
        DefaultDelegationTokenManager.checkProviderAndReceiverConsistency(
                Collections.emptyMap(), Collections.emptyMap());
    }

    @Test
    void checkProviderAndReceiverConsistencyShouldThrowWhenMissingReceiver() {
        Map<String, DelegationTokenProvider> providers = new HashMap<>();
        providers.put("test", new TestDelegationTokenProvider());

        assertThatThrownBy(
                        () ->
                                DefaultDelegationTokenManager.checkProviderAndReceiverConsistency(
                                        providers, Collections.emptyMap()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Missing receivers: test");
    }

    @Test
    void checkProviderAndReceiverConsistencyShouldThrowWhenMissingProvider() {
        Map<String, DelegationTokenReceiver> receivers = new HashMap<>();
        receivers.put("test", new TestDelegationTokenReceiver());

        assertThatThrownBy(
                        () ->
                                DefaultDelegationTokenManager.checkProviderAndReceiverConsistency(
                                        Collections.emptyMap(), receivers))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Missing providers: test");
    }

    @Test
    void checkProviderAndReceiverConsistencyShouldNotThrowWhenBothLoaded() {
        Map<String, DelegationTokenProvider> providers = new HashMap<>();
        providers.put("test", new TestDelegationTokenProvider());
        Map<String, DelegationTokenReceiver> receivers = new HashMap<>();
        receivers.put("test", new TestDelegationTokenReceiver());

        DefaultDelegationTokenManager.checkProviderAndReceiverConsistency(providers, receivers);

        assertThat(providers).hasSize(1);
        assertThat(providers.containsKey("test")).isTrue();
        assertThat(receivers).hasSize(1);
        assertThat(receivers.containsKey("test")).isTrue();
    }

    @Test
    void checkSamePrefixedProvidersShouldNotGiveErrorsWhenNoSamePrefix() {
        Map<String, DelegationTokenProvider> providers = new HashMap<>();
        providers.put("s3-hadoop", new TestDelegationTokenProvider());
        Set<String> warnings = new HashSet<>();
        DefaultDelegationTokenManager.checkSamePrefixedProviders(providers, warnings);
        assertThat(warnings.isEmpty()).isTrue();
    }

    @Test
    void checkSamePrefixedProvidersShouldGiveErrorsWhenSamePrefix() {
        Map<String, DelegationTokenProvider> providers = new HashMap<>();
        providers.put("s3-hadoop", new TestDelegationTokenProvider());
        providers.put("s3-presto", new TestDelegationTokenProvider());
        Set<String> warnings = new HashSet<>();
        DefaultDelegationTokenManager.checkSamePrefixedProviders(providers, warnings);
        assertThat(warnings).hasSize(1);
        assertThat(warnings.iterator().next())
                .isEqualTo(
                        "Multiple providers loaded with the same prefix: s3. This might lead to unintended consequences, please consider using only one of them.");
    }

    @Test
    void startTokensUpdateShouldScheduleRenewal() {
        final ManuallyTriggeredScheduledExecutor scheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();
        final ManuallyTriggeredScheduledExecutorService scheduler =
                new ManuallyTriggeredScheduledExecutorService();

        ExceptionThrowingDelegationTokenProvider.addToken.set(true);
        Configuration configuration = new Configuration();
        configuration.setBoolean(CONFIG_PREFIX + ".throw.enabled", true);
        AtomicInteger startTokensUpdateCallCount = new AtomicInteger(0);
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(
                        configuration, null, scheduledExecutor, scheduler) {
                    @Override
                    void startTokensUpdate() {
                        startTokensUpdateCallCount.incrementAndGet();
                        super.startTokensUpdate();
                    }
                };

        delegationTokenManager.startTokensUpdate();
        ExceptionThrowingDelegationTokenProvider.throwInUsage.set(true);
        scheduledExecutor.triggerScheduledTasks();
        scheduler.triggerAll();
        ExceptionThrowingDelegationTokenProvider.throwInUsage.set(false);
        scheduledExecutor.triggerScheduledTasks();
        scheduler.triggerAll();
        delegationTokenManager.stopTokensUpdate();

        assertThat(startTokensUpdateCallCount.get()).isEqualTo(3);
    }

    @Test
    void calculateRenewalDelayShouldConsiderRenewalRatio() {
        Configuration configuration = new Configuration();
        configuration.setBoolean(CONFIG_PREFIX + ".throw.enabled", false);
        configuration.set(DELEGATION_TOKENS_RENEWAL_TIME_RATIO, 0.5);
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(configuration, null, null, null);

        Clock constantClock = Clock.fixed(ofEpochMilli(100), ZoneId.systemDefault());
        assertThat(delegationTokenManager.calculateRenewalDelay(constantClock, 200)).isEqualTo(50);
    }
}
