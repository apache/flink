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

package org.apache.flink.runtime.security;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.runtime.security.contexts.AnotherCompatibleTestSecurityContextFactory;
import org.apache.flink.runtime.security.contexts.HadoopSecurityContextFactory;
import org.apache.flink.runtime.security.contexts.IncompatibleTestSecurityContextFactory;
import org.apache.flink.runtime.security.contexts.LinkageErrorSecurityContextFactory;
import org.apache.flink.runtime.security.contexts.NoOpSecurityContext;
import org.apache.flink.runtime.security.contexts.NoOpSecurityContextFactory;
import org.apache.flink.runtime.security.contexts.SecurityContext;
import org.apache.flink.runtime.security.contexts.TestSecurityContextFactory;
import org.apache.flink.runtime.security.modules.TestSecurityModuleFactory;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link SecurityUtils}. */
class SecurityUtilsTest {

    @AfterAll
    static void afterAll() {
        SecurityUtils.uninstall();
    }

    @Test
    void testModuleInstall() throws Exception {
        SecurityUtils.install(testSecurityConfiguration(new Configuration()));
        assertThat(SecurityUtils.getInstalledModules()).hasSize(1);
        TestSecurityModuleFactory.TestSecurityModule testModule =
                (TestSecurityModuleFactory.TestSecurityModule)
                        SecurityUtils.getInstalledModules().get(0);
        assertThat(testModule.installed).isTrue();

        SecurityUtils.uninstall();
        assertThat(SecurityUtils.getInstalledModules()).isNull();
        assertThat(testModule.installed).isFalse();
    }

    @Test
    void testSecurityContext() throws Exception {
        assertInstalledContext(
                testSecurityConfiguration(new Configuration()),
                TestSecurityContextFactory.TestSecurityContext.class);
    }

    @Test
    void testLinkageErrorShouldFallbackToSecond() throws Exception {
        assertInstalledContext(
                securityConfigurationWithContextFactories(
                        LinkageErrorSecurityContextFactory.class, TestSecurityContextFactory.class),
                TestSecurityContextFactory.TestSecurityContext.class);
    }

    /** Verify that we fall back to a second configuration if the first one is incompatible. */
    @Test
    void testSecurityContextShouldFallbackToSecond() throws Exception {
        assertInstalledContext(
                securityConfigurationWithContextFactories(
                        IncompatibleTestSecurityContextFactory.class,
                        TestSecurityContextFactory.class),
                TestSecurityContextFactory.TestSecurityContext.class);
    }

    /** Verify that we pick the first valid security context. */
    @Test
    void testSecurityContextShouldPickFirstIfBothCompatible() throws Exception {
        assertInstalledContext(
                securityConfigurationWithContextFactories(
                        AnotherCompatibleTestSecurityContextFactory.class,
                        TestSecurityContextFactory.class),
                AnotherCompatibleTestSecurityContextFactory.TestSecurityContext.class);

        assertInstalledContext(
                securityConfigurationWithContextFactories(
                        TestSecurityContextFactory.class,
                        AnotherCompatibleTestSecurityContextFactory.class),
                TestSecurityContextFactory.TestSecurityContext.class);
    }

    @Test
    void testSecurityFactoriesDefaultConfig() {
        SecurityConfiguration testSecurityConf = new SecurityConfiguration(new Configuration());
        // should load the default context security module factories.
        assertThat(testSecurityConf.getSecurityContextFactories())
                .containsExactly(
                        HadoopSecurityContextFactory.class.getCanonicalName(),
                        NoOpSecurityContextFactory.class.getCanonicalName());
        // should load 3 default security module factories.
        assertThat(testSecurityConf.getSecurityModuleFactories()).hasSize(3);
    }

    @Test
    void testSecurityFactoriesUserConfig() {
        Configuration testFlinkConf = new Configuration();
        testFlinkConf.set(
                SecurityOptions.SECURITY_CONTEXT_FACTORY_CLASSES,
                List.of(TestSecurityContextFactory.class.getCanonicalName()));
        testFlinkConf.set(
                SecurityOptions.SECURITY_MODULE_FACTORY_CLASSES,
                List.of(TestSecurityModuleFactory.class.getCanonicalName()));
        SecurityConfiguration testSecurityConf = new SecurityConfiguration(testFlinkConf);

        // should pick up the SecurityOptions to override default factories.
        assertThat(testSecurityConf.getSecurityContextFactories())
                .containsExactly(TestSecurityContextFactory.class.getCanonicalName());
        assertThat(testSecurityConf.getSecurityModuleFactories())
                .containsExactly(TestSecurityModuleFactory.class.getCanonicalName());
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "Foo bar,Client",
                "Foo bar , Client",
                " Foo bar , Client ",
                "Foo bar,,Client",
                "Foo bar, ,, Client,"
            })
    void testKerberosLoginContextParsing(String loginContexts) {
        Configuration testFlinkConf = new Configuration();
        testFlinkConf.set(SecurityOptions.KERBEROS_LOGIN_CONTEXTS, loginContexts);

        assertThat(testSecurityConfiguration(testFlinkConf).getLoginContextNames())
                .containsExactly("Foo bar", "Client");
    }

    private static SecurityConfiguration testSecurityConfiguration(Configuration flinkConf) {
        return new SecurityConfiguration(
                flinkConf,
                List.of(TestSecurityContextFactory.class.getCanonicalName()),
                List.of(TestSecurityModuleFactory.class.getCanonicalName()));
    }

    private static SecurityConfiguration securityConfigurationWithContextFactories(
            Class<?> first, Class<?> second) {
        Configuration flinkConf = new Configuration();
        flinkConf.set(
                SecurityOptions.SECURITY_CONTEXT_FACTORY_CLASSES,
                List.of(first.getCanonicalName(), second.getCanonicalName()));
        return new SecurityConfiguration(flinkConf);
    }

    /** Installs the configuration, checks the installed context and uninstalls it again. */
    private static void assertInstalledContext(
            SecurityConfiguration securityConfiguration,
            Class<? extends SecurityContext> expectedContext)
            throws Exception {
        SecurityUtils.install(securityConfiguration);
        assertThat(SecurityUtils.getInstalledContext()).isExactlyInstanceOf(expectedContext);

        SecurityUtils.uninstall();
        assertThat(SecurityUtils.getInstalledContext())
                .isExactlyInstanceOf(NoOpSecurityContext.class);
    }
}
