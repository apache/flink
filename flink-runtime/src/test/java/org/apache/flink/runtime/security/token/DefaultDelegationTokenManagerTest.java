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
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.TestingPluginManager;
import org.apache.flink.core.security.token.DelegationTokenProvider;
import org.apache.flink.core.security.token.DelegationTokenReceiver;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.time.Instant.ofEpochMilli;
import static org.apache.flink.configuration.ConfigurationUtils.getBooleanConfigOption;
import static org.apache.flink.configuration.SecurityOptions.DELEGATION_TOKENS_RENEWAL_RETRY_INITIAL_BACKOFF;
import static org.apache.flink.configuration.SecurityOptions.DELEGATION_TOKENS_RENEWAL_RETRY_MAX_BACKOFF;
import static org.apache.flink.configuration.SecurityOptions.DELEGATION_TOKENS_RENEWAL_TIME_RATIO;
import static org.apache.flink.core.security.token.DelegationTokenProvider.CONFIG_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for {@link DelegationTokenManager}. */
public class DefaultDelegationTokenManagerTest {

    @Test
    public void configurationIsNullMustFailFast() {
        assertThatThrownBy(() -> new DefaultDelegationTokenManager(null, null, null, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Flink configuration must not be null");
    }

    @Test
    void providerWithoutReceiverFailsTheConstruction() {
        assertThatThrownBy(
                        () ->
                                new DefaultDelegationTokenManager(
                                        disabled("hadoopfs", "hbase", "test", "throw"),
                                        pluginManager(
                                                List.of(
                                                        TestingDelegationTokenProvider
                                                                .withoutExpiry("lonely")),
                                                List.of()),
                                        null,
                                        null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Missing receivers: lonely");
    }

    @Test
    void oneShotObtainHandsTheTokensOfAllProvidersToTheCaller() throws Exception {
        final TestingDelegationTokenProvider a =
                TestingDelegationTokenProvider.validUntil("a", 100L);
        final TestingDelegationTokenProvider b = TestingDelegationTokenProvider.withoutExpiry("b");
        final DelegationTokenContainer container = new DelegationTokenContainer();

        manager(a, b).obtainDelegationTokens(container);

        assertThat(container.getTokens()).containsOnlyKeys("a", "b");
        assertThat(container.getTokens().get("a")).isEqualTo(a.tokens());
        assertThat(container.getTokens().get("b")).isEqualTo(b.tokens());
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
    public void calculateObtainDelayShouldConsiderTheTimeRatio() {
        Configuration configuration = new Configuration();
        configuration.set(getBooleanConfigOption(CONFIG_PREFIX + ".throw.enabled"), false);
        configuration.set(DELEGATION_TOKENS_RENEWAL_TIME_RATIO, 0.5);
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(configuration, null, null, null);

        Clock constantClock = Clock.fixed(ofEpochMilli(100), ZoneId.systemDefault());
        assertEquals(50, delegationTokenManager.calculateObtainDelay(constantClock, 200));
    }

    @Test
    public void calculateNextRetryDelayShouldDoubleOnConsecutiveFailures() {
        long initialMs = Duration.ofSeconds(10).toMillis();
        Configuration configuration = new Configuration();
        configuration.set(getBooleanConfigOption(CONFIG_PREFIX + ".throw.enabled"), false);
        configuration.set(DELEGATION_TOKENS_RENEWAL_RETRY_INITIAL_BACKOFF, Duration.ofSeconds(10));
        configuration.set(DELEGATION_TOKENS_RENEWAL_RETRY_MAX_BACKOFF, Duration.ofMinutes(5));
        DefaultDelegationTokenManager manager =
                new DefaultDelegationTokenManager(configuration, null, null, null);

        Clock clock = Clock.fixed(ofEpochMilli(0), ZoneId.systemDefault());
        long delay1 = manager.calculateNextRetryDelay(clock);
        long delay2 = manager.calculateNextRetryDelay(clock);
        long delay3 = manager.calculateNextRetryDelay(clock);

        // Each delay should be within [0, 2 * initial * 2^(n-1)] accounting for ±50% jitter.
        assertTrue(delay1 >= 0 && delay1 <= initialMs * 2);
        assertTrue(delay2 >= 0 && delay2 <= initialMs * 4);
        assertTrue(delay3 >= 0 && delay3 <= initialMs * 8);
        // The base must have doubled: currentRetryBackoff after 3 calls is min(80s, 5min) = 80s.
        assertEquals(Duration.ofSeconds(80).toMillis(), manager.currentRetryBackoff);
    }

    @Test
    public void calculateNextRetryDelayShouldResetAfterSuccess() {
        long initialMs = Duration.ofSeconds(10).toMillis();
        Configuration configuration = new Configuration();
        configuration.set(getBooleanConfigOption(CONFIG_PREFIX + ".throw.enabled"), false);
        configuration.set(DELEGATION_TOKENS_RENEWAL_RETRY_INITIAL_BACKOFF, Duration.ofSeconds(10));
        configuration.set(DELEGATION_TOKENS_RENEWAL_RETRY_MAX_BACKOFF, Duration.ofMinutes(5));
        DefaultDelegationTokenManager manager =
                new DefaultDelegationTokenManager(configuration, null, null, null);

        // Ramp up the backoff via two failures.
        Clock clock = Clock.fixed(ofEpochMilli(0), ZoneId.systemDefault());
        manager.calculateNextRetryDelay(clock);
        manager.calculateNextRetryDelay(clock);
        // Simulate success: reset currentRetryBackoff (as a successful obtain would).
        manager.currentRetryBackoff = initialMs;

        long delayAfterReset = manager.calculateNextRetryDelay(clock);
        assertTrue(delayAfterReset >= 0 && delayAfterReset <= initialMs * 2);
        assertEquals(initialMs * 2, manager.currentRetryBackoff);
    }

    @Test
    public void calculateNextRetryDelayShouldCapToTtlBound() {
        Configuration configuration = new Configuration();
        configuration.set(getBooleanConfigOption(CONFIG_PREFIX + ".throw.enabled"), false);
        configuration.set(DELEGATION_TOKENS_RENEWAL_RETRY_INITIAL_BACKOFF, Duration.ofSeconds(10));
        configuration.set(DELEGATION_TOKENS_RENEWAL_RETRY_MAX_BACKOFF, Duration.ofMinutes(5));
        DefaultDelegationTokenManager manager =
                new DefaultDelegationTokenManager(configuration, null, null, null);

        // Simulate a failure close to token expiry (30 s remaining). The delay must be capped
        // so that the retry happens while the token is still valid (at most 30 s / 3 = 10 s).
        Clock clock = Clock.fixed(ofEpochMilli(0), ZoneId.systemDefault());
        manager.lastKnownExpiry = Duration.ofSeconds(30).toMillis();

        long delay = manager.calculateNextRetryDelay(clock);

        // Delay must not exceed the TTL cap (30 s / 3 = 10 s), with jitter the max is 10 s.
        assertTrue(delay <= Duration.ofSeconds(10).toMillis());
        assertTrue(delay >= 0);
    }

    /** Disables the providers and receivers which the service loader finds, by service name. */
    private static Configuration disabled(String... serviceNames) {
        final Configuration configuration = new Configuration();
        for (String serviceName : serviceNames) {
            configuration.set(
                    getBooleanConfigOption(CONFIG_PREFIX + "." + serviceName + ".enabled"), false);
        }
        return configuration;
    }

    /** A manager with exactly these providers, each with a receiver of its own. */
    private static DefaultDelegationTokenManager manager(
            TestingDelegationTokenProvider... providers) {
        final List<DelegationTokenProvider> providerList = List.of(providers);
        final List<DelegationTokenReceiver> receivers =
                providerList.stream()
                        .map(provider -> new TestingDelegationTokenReceiver(provider.serviceName()))
                        .collect(Collectors.toList());
        return new DefaultDelegationTokenManager(
                disabled("hadoopfs", "hbase", "test", "throw"),
                pluginManager(providerList, receivers),
                null,
                null);
    }

    private static PluginManager pluginManager(
            List<DelegationTokenProvider> providers, List<DelegationTokenReceiver> receivers) {
        final Map<Class<?>, Iterator<?>> plugins = new HashMap<>();
        plugins.put(DelegationTokenProvider.class, providers.iterator());
        plugins.put(DelegationTokenReceiver.class, receivers.iterator());
        return new TestingPluginManager(plugins);
    }
}
