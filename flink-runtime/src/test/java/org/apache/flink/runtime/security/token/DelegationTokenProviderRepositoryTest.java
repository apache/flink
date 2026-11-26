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
import org.apache.flink.testutils.logging.LoggerAuditingExtension;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.event.Level;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.configuration.ConfigurationUtils.getBooleanConfigOption;
import static org.apache.flink.core.security.token.DelegationTokenProvider.CONFIG_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DelegationTokenProviderRepository}. */
class DelegationTokenProviderRepositoryTest {

    @RegisterExtension
    private final LoggerAuditingExtension warnings =
            new LoggerAuditingExtension(DelegationTokenProviderRepository.class, Level.WARN);

    @BeforeEach
    void beforeEach() {
        ExceptionThrowingDelegationTokenProvider.reset();
    }

    @AfterEach
    void afterEach() {
        ExceptionThrowingDelegationTokenProvider.reset();
    }

    // ------------------------------------------------------------------------------------------
    // Loading
    // ------------------------------------------------------------------------------------------

    @Test
    void providerIsEnabledByDefault() {
        assertThat(DelegationTokenProviderRepository.isProviderEnabled(new Configuration(), "test"))
                .isTrue();
    }

    @Test
    void providerIsDisabledByItsConfigOption() {
        final Configuration configuration = new Configuration();
        configuration.set(getBooleanConfigOption(CONFIG_PREFIX + ".test.enabled"), false);

        assertThat(DelegationTokenProviderRepository.isProviderEnabled(configuration, "test"))
                .isFalse();
    }

    @Test
    void configurationIsNullMustFailFast() {
        assertThatThrownBy(() -> new DelegationTokenProviderRepository(null, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Flink configuration must not be null");
    }

    @Test
    void allEnabledProvidersOfTheServiceLoaderAreLoaded() {
        final Configuration configuration = new Configuration();
        configuration.set(getBooleanConfigOption(CONFIG_PREFIX + ".throw.enabled"), false);

        final DelegationTokenProviderRepository repository =
                new DelegationTokenProviderRepository(configuration, null);

        assertThat(repository.delegationTokenProviders)
                .containsOnlyKeys("hadoopfs", "hbase", "test");
        assertThat(repository.isProviderLoaded("hadoopfs")).isTrue();
        assertThat(repository.isProviderLoaded("hbase")).isTrue();
        assertThat(repository.isProviderLoaded("test")).isTrue();
        assertThat(ExceptionThrowingDelegationTokenProvider.constructed.get()).isTrue();
        assertThat(repository.isProviderLoaded("throw")).isFalse();
    }

    @Test
    void providersOfThePluginManagerAreLoaded() {
        final TestingDelegationTokenProvider provider =
                TestingDelegationTokenProvider.withoutExpiry("plugin");

        final DelegationTokenProviderRepository repository = repository(provider);

        assertThat(repository.delegationTokenProviders).containsOnlyKeys("plugin");
        assertThat(repository.delegationTokenProviders.get("plugin")).isSameAs(provider);
    }

    @Test
    void oneProviderThrowsExceptionMustFailFast() {
        ExceptionThrowingDelegationTokenProvider.throwInInit.set(true);

        assertThatThrownBy(() -> new DelegationTokenProviderRepository(new Configuration(), null))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasRootCauseInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void errorInTheInitOfAProviderFailsTheConstructionWithTheOriginalCause() {
        final NoClassDefFoundError error = new NoClassDefFoundError("provider library is missing");

        assertThatThrownBy(
                        () ->
                                repository(
                                        TestingDelegationTokenProvider.withoutExpiry("broken")
                                                .failingInitWith(error)))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasRootCause(error);
    }

    @Test
    void providersWithTheSameServiceNameFailTheConstruction() {
        // "test" is also the service name of the provider which the service loader finds
        final Configuration configuration = disabled("hadoopfs", "hbase", "throw");
        final PluginManager pluginManager =
                pluginManager(List.of(TestingDelegationTokenProvider.withoutExpiry("test")));

        assertThatThrownBy(
                        () -> new DelegationTokenProviderRepository(configuration, pluginManager))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasRootCauseInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage(
                        "Delegation token provider with service name test has multiple implementations");
    }

    @Test
    void providersWithTheSamePrefixAreWarnedAbout() {
        repository(
                TestingDelegationTokenProvider.withoutExpiry("s3-hadoop"),
                TestingDelegationTokenProvider.withoutExpiry("s3-presto"));

        assertThat(warnings.getMessages())
                .containsExactly(
                        "Multiple providers loaded with the same prefix: s3. This might lead to unintended consequences, please consider using only one of them.");
    }

    @Test
    void checkSamePrefixedProvidersShouldNotGiveErrorsWhenNoSamePrefix() {
        final Map<String, DelegationTokenProvider> providers = new HashMap<>();
        providers.put("s3-hadoop", new TestDelegationTokenProvider());
        final Set<String> warnings = new HashSet<>();

        DelegationTokenProviderRepository.checkSamePrefixedProviders(providers, warnings);

        assertThat(warnings).isEmpty();
    }

    @Test
    void checkSamePrefixedProvidersShouldGiveErrorsWhenSamePrefix() {
        final Map<String, DelegationTokenProvider> providers = new HashMap<>();
        providers.put("s3-hadoop", new TestDelegationTokenProvider());
        providers.put("s3-presto", new TestDelegationTokenProvider());
        final Set<String> warnings = new HashSet<>();

        DelegationTokenProviderRepository.checkSamePrefixedProviders(providers, warnings);

        assertThat(warnings)
                .containsExactly(
                        "Multiple providers loaded with the same prefix: s3. This might lead to unintended consequences, please consider using only one of them.");
    }

    // ------------------------------------------------------------------------------------------
    // Obtaining
    // ------------------------------------------------------------------------------------------

    @Test
    void expiryIsTheEarliestOfTheProviders() {
        final DelegationTokenProviderRepository repository =
                repository(
                        TestingDelegationTokenProvider.validUntil("a", 300L),
                        TestingDelegationTokenProvider.validUntil("b", 100L),
                        TestingDelegationTokenProvider.validUntil("c", 200L));

        assertThat(repository.obtainDelegationTokensAndGetExpiry(new DelegationTokenContainer()))
                .hasValue(100L);
    }

    @Test
    void providerWithoutExpiryDoesNotInfluenceTheExpiry() {
        final DelegationTokenProviderRepository repository =
                repository(
                        TestingDelegationTokenProvider.withoutExpiry("a"),
                        TestingDelegationTokenProvider.validUntil("b", 200L));

        assertThat(repository.obtainDelegationTokensAndGetExpiry(new DelegationTokenContainer()))
                .hasValue(200L);
    }

    @Test
    void thereIsNoExpiryWhenNoProviderReportsOne() {
        final DelegationTokenProviderRepository repository =
                repository(TestingDelegationTokenProvider.withoutExpiry("a"));

        assertThat(repository.obtainDelegationTokensAndGetExpiry(new DelegationTokenContainer()))
                .isEmpty();
    }

    @Test
    void providersWhichDoNotNeedTokensAreNotAsked() {
        final TestingDelegationTokenProvider notNeeded =
                TestingDelegationTokenProvider.validUntil("a", 100L).notRequiringTokens();
        final TestingDelegationTokenProvider needed =
                TestingDelegationTokenProvider.validUntil("b", 200L);
        final DelegationTokenContainer container = new DelegationTokenContainer();

        final Optional<Long> expiry =
                repository(notNeeded, needed).obtainDelegationTokensAndGetExpiry(container);

        assertThat(notNeeded.obtainCalls()).isZero();
        assertThat(needed.obtainCalls()).isEqualTo(1);
        assertThat(container.getTokens()).containsOnlyKeys("b");
        assertThat(expiry).hasValue(200L);
    }

    @Test
    void tokensAreStoredUnderTheServiceNameOfTheirProvider() {
        final TestingDelegationTokenProvider a =
                TestingDelegationTokenProvider.validUntil("a", 100L);
        final TestingDelegationTokenProvider b = TestingDelegationTokenProvider.withoutExpiry("b");
        final DelegationTokenContainer container = new DelegationTokenContainer();

        repository(a, b).obtainDelegationTokensAndGetExpiry(container);

        assertThat(container.getTokens()).containsOnlyKeys("a", "b");
        assertThat(container.getTokens().get("a")).isEqualTo(a.tokens());
        assertThat(container.getTokens().get("b")).isEqualTo(b.tokens());
    }

    @Test
    void providerWhichReturnsNoTokensFailsTheObtain() {
        final DelegationTokenProviderRepository repository =
                repository(TestingDelegationTokenProvider.withoutExpiry("a").returningNoTokens());

        assertThatThrownBy(
                        () ->
                                repository.obtainDelegationTokensAndGetExpiry(
                                        new DelegationTokenContainer()))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasRootCauseInstanceOf(NullPointerException.class)
                .hasRootCauseMessage("Obtained delegation tokens must not be null");
    }

    /** Disables the providers which the service loader finds, by service name. */
    private static Configuration disabled(String... serviceNames) {
        final Configuration configuration = new Configuration();
        for (String serviceName : serviceNames) {
            configuration.set(
                    getBooleanConfigOption(CONFIG_PREFIX + "." + serviceName + ".enabled"), false);
        }
        return configuration;
    }

    /** A repository with exactly these providers. */
    private static DelegationTokenProviderRepository repository(
            TestingDelegationTokenProvider... providers) {
        return new DelegationTokenProviderRepository(
                disabled("hadoopfs", "hbase", "test", "throw"), pluginManager(List.of(providers)));
    }

    private static PluginManager pluginManager(List<DelegationTokenProvider> providers) {
        final Map<Class<?>, Iterator<?>> plugins = new HashMap<>();
        plugins.put(DelegationTokenProvider.class, providers.iterator());
        return new TestingPluginManager(plugins);
    }
}
