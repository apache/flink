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
import org.apache.flink.core.testutils.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicInteger;

import static java.time.Instant.ofEpochMilli;
import static org.apache.flink.configuration.SecurityOptions.DELEGATION_TOKENS_RENEWAL_TIME_RATIO;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for {@link DelegationTokenManager}. */
public class DefaultDelegationTokenManagerTest {

    @BeforeEach
    public void beforeEach() {
        ExceptionThrowingDelegationTokenProvider.reset();
    }

    @AfterAll
    public static void afterAll() {
        ExceptionThrowingDelegationTokenProvider.reset();
    }

    @Test
    public void isProviderEnabledMustGiveBackTrueByDefault() {
        Configuration configuration = new Configuration();
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(configuration, null, null);

        assertTrue(delegationTokenManager.isProviderEnabled("test"));
    }

    @Test
    public void isProviderEnabledMustGiveBackFalseWhenDisabled() {
        Configuration configuration = new Configuration();
        configuration.setBoolean("security.delegation.token.provider.test.enabled", false);
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(configuration, null, null);

        assertFalse(delegationTokenManager.isProviderEnabled("test"));
    }

    @Test
    public void configurationIsNullMustFailFast() {
        assertThrows(Exception.class, () -> new DefaultDelegationTokenManager(null, null, null));
    }

    @Test
    public void testAllProvidersLoaded() {
        Configuration configuration = new Configuration();
        configuration.setBoolean("security.delegation.token.provider.throw.enabled", false);
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(configuration, null, null);

        assertEquals(3, delegationTokenManager.delegationTokenProviders.size());
        assertTrue(delegationTokenManager.isProviderLoaded("hadoopfs"));
        assertTrue(delegationTokenManager.isProviderLoaded("hbase"));
        assertTrue(delegationTokenManager.isProviderLoaded("test"));
        assertTrue(ExceptionThrowingDelegationTokenProvider.constructed);
        assertFalse(delegationTokenManager.isProviderLoaded("throw"));
    }

    @Test
    public void startTokensUpdateShouldScheduleRenewal() {
        final ManuallyTriggeredScheduledExecutor scheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();
        final ManuallyTriggeredScheduledExecutorService scheduler =
                new ManuallyTriggeredScheduledExecutorService();

        ExceptionThrowingDelegationTokenProvider.addToken = true;
        Configuration configuration = new Configuration();
        configuration.setBoolean("security.delegation.token.provider.throw.enabled", true);
        AtomicInteger startTokensUpdateCallCount = new AtomicInteger(0);
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(configuration, scheduledExecutor, scheduler) {
                    @Override
                    void startTokensUpdate() {
                        startTokensUpdateCallCount.incrementAndGet();
                        super.startTokensUpdate();
                    }
                };

        delegationTokenManager.startTokensUpdate();
        ExceptionThrowingDelegationTokenProvider.throwInUsage = true;
        scheduledExecutor.triggerScheduledTasks();
        scheduler.triggerAll();
        ExceptionThrowingDelegationTokenProvider.throwInUsage = false;
        scheduledExecutor.triggerScheduledTasks();
        scheduler.triggerAll();
        delegationTokenManager.stopTokensUpdate();

        assertEquals(3, startTokensUpdateCallCount.get());
    }

    @Test
    public void calculateRenewalDelayShouldConsiderRenewalRatio() {
        Configuration configuration = new Configuration();
        configuration.setBoolean("security.delegation.token.provider.throw.enabled", false);
        configuration.set(DELEGATION_TOKENS_RENEWAL_TIME_RATIO, 0.5);
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(configuration, null, null);

        Clock constantClock = Clock.fixed(ofEpochMilli(100), ZoneId.systemDefault());
        assertEquals(50, delegationTokenManager.calculateRenewalDelay(constantClock, 200));
    }
}
