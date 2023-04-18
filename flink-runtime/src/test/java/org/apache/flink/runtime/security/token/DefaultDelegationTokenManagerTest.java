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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for {@link DelegationTokenManager}. */
public class DefaultDelegationTokenManagerTest {

    @BeforeEach
    public void beforeEach() {
        ExceptionThrowingDelegationTokenProvider.reset();
        ExceptionThrowingDelegationTokenReceiver.reset();
    }

    @AfterEach
    public void afterEach() {
        ExceptionThrowingDelegationTokenProvider.reset();
        ExceptionThrowingDelegationTokenReceiver.reset();
    }

    @Test
    public void isProviderEnabledMustGiveBackTrueByDefault() {
        Configuration configuration = new Configuration();

        assertTrue(DefaultDelegationTokenManager.isProviderEnabled(configuration, "test"));
    }

    @Test
    public void isProviderEnabledMustGiveBackFalseWhenDisabled() {
        Configuration configuration = new Configuration();
        configuration.setBoolean(CONFIG_PREFIX + ".test.enabled", false);

        assertFalse(DefaultDelegationTokenManager.isProviderEnabled(configuration, "test"));
    }

    @Test
    public void configurationIsNullMustFailFast() {
        assertThrows(
                Exception.class, () -> new DefaultDelegationTokenManager(null, null, null, null));
    }

    @Test
    public void oneProviderThrowsExceptionMustFailFast() {
        assertThrows(
                Exception.class,
                () -> {
                    ExceptionThrowingDelegationTokenProvider.throwInInit.set(true);
                    new DefaultDelegationTokenManager(new Configuration(), null, null, null);
                });
    }

    @Test
    public void testAllProvidersLoaded() {
        Configuration configuration = new Configuration();
        configuration.setBoolean(CONFIG_PREFIX + ".throw.enabled", false);
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(configuration, null, null, null);

        assertEquals(3, delegationTokenManager.delegationTokenProviders.size());

        assertTrue(delegationTokenManager.isProviderLoaded("hadoopfs"));
        assertTrue(delegationTokenManager.isReceiverLoaded("hadoopfs"));

        assertTrue(delegationTokenManager.isProviderLoaded("hbase"));
        assertTrue(delegationTokenManager.isReceiverLoaded("hbase"));

        assertTrue(delegationTokenManager.isProviderLoaded("test"));
        assertTrue(delegationTokenManager.isReceiverLoaded("test"));

        assertTrue(ExceptionThrowingDelegationTokenProvider.constructed.get());
        assertTrue(ExceptionThrowingDelegationTokenReceiver.constructed.get());
        assertFalse(delegationTokenManager.isProviderLoaded("throw"));
        assertFalse(delegationTokenManager.isReceiverLoaded("throw"));
    }

    @Test
    public void checkProviderAndReceiverConsistencyShouldNotThrowWhenNothingLoaded() {
        DefaultDelegationTokenManager.checkProviderAndReceiverConsistency(
                Collections.emptyMap(), Collections.emptyMap());
    }

    @Test
    public void checkProviderAndReceiverConsistencyShouldThrowWhenMissingReceiver() {
        Map<String, DelegationTokenProvider> providers = new HashMap<>();
        providers.put("test", new TestDelegationTokenProvider());

        IllegalStateException e =
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                DefaultDelegationTokenManager.checkProviderAndReceiverConsistency(
                                        providers, Collections.emptyMap()));
        assertTrue(e.getMessage().contains("Missing receivers: test"));
    }

    @Test
    public void checkProviderAndReceiverConsistencyShouldThrowWhenMissingProvider() {
        Map<String, DelegationTokenReceiver> receivers = new HashMap<>();
        receivers.put("test", new TestDelegationTokenReceiver());

        IllegalStateException e =
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                DefaultDelegationTokenManager.checkProviderAndReceiverConsistency(
                                        Collections.emptyMap(), receivers));
        assertTrue(e.getMessage().contains("Missing providers: test"));
    }

    @Test
    public void checkProviderAndReceiverConsistencyShouldNotThrowWhenBothLoaded() {
        Map<String, DelegationTokenProvider> providers = new HashMap<>();
        providers.put("test", new TestDelegationTokenProvider());
        Map<String, DelegationTokenReceiver> receivers = new HashMap<>();
        receivers.put("test", new TestDelegationTokenReceiver());

        DefaultDelegationTokenManager.checkProviderAndReceiverConsistency(providers, receivers);

        assertEquals(1, providers.size());
        assertTrue(providers.containsKey("test"));
        assertEquals(1, receivers.size());
        assertTrue(receivers.containsKey("test"));
    }

    @Test
    public void checkSamePrefixedProvidersShouldNotGiveErrorsWhenNoSamePrefix() {
        Map<String, DelegationTokenProvider> providers = new HashMap<>();
        providers.put("s3-hadoop", new TestDelegationTokenProvider());
        Set<String> warnings = new HashSet<>();
        DefaultDelegationTokenManager.checkSamePrefixedProviders(providers, warnings);
        assertTrue(warnings.isEmpty());
    }

    @Test
    public void checkSamePrefixedProvidersShouldGiveErrorsWhenSamePrefix() {
        Map<String, DelegationTokenProvider> providers = new HashMap<>();
        providers.put("s3-hadoop", new TestDelegationTokenProvider());
        providers.put("s3-presto", new TestDelegationTokenProvider());
        Set<String> warnings = new HashSet<>();
        DefaultDelegationTokenManager.checkSamePrefixedProviders(providers, warnings);
        assertEquals(1, warnings.size());
        assertEquals(
                "Multiple providers loaded with the same prefix: s3. This might lead to unintended consequences, please consider using only one of them.",
                warnings.iterator().next());
    }

    @Test
    public void startTokensUpdateShouldScheduleRenewal() {
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

        assertEquals(3, startTokensUpdateCallCount.get());
    }

    @Test
    public void calculateRenewalDelayShouldConsiderRenewalRatio() {
        Configuration configuration = new Configuration();
        configuration.setBoolean(CONFIG_PREFIX + ".throw.enabled", false);
        configuration.set(DELEGATION_TOKENS_RENEWAL_TIME_RATIO, 0.5);
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(configuration, null, null, null);

        Clock constantClock = Clock.fixed(ofEpochMilli(100), ZoneId.systemDefault());
        assertEquals(50, delegationTokenManager.calculateRenewalDelay(constantClock, 200));
    }
}
