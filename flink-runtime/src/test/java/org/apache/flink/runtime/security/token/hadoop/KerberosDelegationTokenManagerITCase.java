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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.runtime.security.token.DelegationTokenManager;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;

import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.time.Clock;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicInteger;

import static java.time.Instant.ofEpochMilli;
import static org.apache.flink.configuration.SecurityOptions.KERBEROS_TOKENS_RENEWAL_TIME_RATIO;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

/**
 * Test for {@link DelegationTokenManager}.
 *
 * <p>This class is an ITCase because the mocking breaks the {@link UserGroupInformation} class for
 * other tests.
 */
public class KerberosDelegationTokenManagerITCase {

    @Test
    public void isProviderEnabledMustGiveBackTrueByDefault() {
        ExceptionThrowingHadoopDelegationTokenProvider.reset();
        Configuration configuration = new Configuration();
        KerberosDelegationTokenManager delegationTokenManager =
                new KerberosDelegationTokenManager(configuration, null, null);

        assertTrue(delegationTokenManager.isProviderEnabled("test"));
    }

    @Test
    public void isProviderEnabledMustGiveBackFalseWhenDisabled() {
        ExceptionThrowingHadoopDelegationTokenProvider.reset();
        Configuration configuration = new Configuration();
        configuration.setBoolean("security.kerberos.token.provider.test.enabled", false);
        KerberosDelegationTokenManager delegationTokenManager =
                new KerberosDelegationTokenManager(configuration, null, null);

        assertFalse(delegationTokenManager.isProviderEnabled("test"));
    }

    @Test
    public void configurationIsNullMustFailFast() {
        assertThrows(Exception.class, () -> new KerberosDelegationTokenManager(null, null, null));
    }

    @Test
    public void oneProviderThrowsExceptionMustFailFast() {
        assertThrows(
                Exception.class,
                () -> {
                    try {
                        ExceptionThrowingHadoopDelegationTokenProvider.reset();
                        ExceptionThrowingHadoopDelegationTokenProvider.throwInInit = true;
                        new KerberosDelegationTokenManager(new Configuration(), null, null);
                    } finally {
                        ExceptionThrowingHadoopDelegationTokenProvider.reset();
                    }
                });
    }

    @Test
    public void testAllProvidersLoaded() {
        ExceptionThrowingHadoopDelegationTokenProvider.reset();
        Configuration configuration = new Configuration();
        configuration.setBoolean("security.kerberos.token.provider.throw.enabled", false);
        KerberosDelegationTokenManager delegationTokenManager =
                new KerberosDelegationTokenManager(configuration, null, null);

        assertEquals(3, delegationTokenManager.delegationTokenProviders.size());
        assertTrue(delegationTokenManager.isProviderLoaded("hadoopfs"));
        assertTrue(delegationTokenManager.isProviderLoaded("hbase"));
        assertTrue(delegationTokenManager.isProviderLoaded("test"));
        assertTrue(ExceptionThrowingHadoopDelegationTokenProvider.constructed);
        assertFalse(delegationTokenManager.isProviderLoaded("throw"));
    }

    @Test
    public void startTokensUpdateShouldScheduleRenewal() {
        final ManuallyTriggeredScheduledExecutor scheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();
        final ManuallyTriggeredScheduledExecutorService scheduler =
                new ManuallyTriggeredScheduledExecutorService();

        try (MockedStatic<UserGroupInformation> ugi = mockStatic(UserGroupInformation.class)) {
            UserGroupInformation userGroupInformation = mock(UserGroupInformation.class);
            ugi.when(UserGroupInformation::getCurrentUser).thenReturn(userGroupInformation);

            ExceptionThrowingHadoopDelegationTokenProvider.reset();
            Configuration configuration = new Configuration();
            configuration.setBoolean("security.kerberos.token.provider.throw.enabled", true);
            AtomicInteger startTokensUpdateCallCount = new AtomicInteger(0);
            KerberosLoginProvider kerberosLoginProvider = new KerberosLoginProvider(configuration);
            KerberosDelegationTokenManager delegationTokenManager =
                    new KerberosDelegationTokenManager(
                            configuration, scheduledExecutor, scheduler) {
                        @Override
                        void startTokensUpdate() {
                            startTokensUpdateCallCount.incrementAndGet();
                            super.startTokensUpdate();
                        }
                    };

            delegationTokenManager.startTokensUpdate();
            ExceptionThrowingHadoopDelegationTokenProvider.throwInUsage = true;
            scheduledExecutor.triggerScheduledTasks();
            scheduler.triggerAll();
            ExceptionThrowingHadoopDelegationTokenProvider.throwInUsage = false;
            scheduledExecutor.triggerScheduledTasks();
            scheduler.triggerAll();
            delegationTokenManager.stopTokensUpdate();

            assertEquals(3, startTokensUpdateCallCount.get());
        }
    }

    @Test
    public void calculateRenewalDelayShouldConsiderRenewalRatio() {
        ExceptionThrowingHadoopDelegationTokenProvider.reset();
        Configuration configuration = new Configuration();
        configuration.setBoolean("security.kerberos.token.provider.throw.enabled", false);
        configuration.set(KERBEROS_TOKENS_RENEWAL_TIME_RATIO, 0.5);
        KerberosDelegationTokenManager delegationTokenManager =
                new KerberosDelegationTokenManager(configuration, null, null);

        Clock constantClock = Clock.fixed(ofEpochMilli(100), ZoneId.systemDefault());
        assertEquals(50, delegationTokenManager.calculateRenewalDelay(constantClock, 200));
    }
}
