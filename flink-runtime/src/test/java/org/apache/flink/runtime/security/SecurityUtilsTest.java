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
import org.apache.flink.runtime.security.contexts.TestSecurityContextFactory;
import org.apache.flink.runtime.security.modules.TestSecurityModuleFactory;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
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
        SecurityConfiguration sc =
                new SecurityConfiguration(
                        new Configuration(),
                        Collections.singletonList(
                                TestSecurityContextFactory.class.getCanonicalName()),
                        Collections.singletonList(
                                TestSecurityModuleFactory.class.getCanonicalName()));

        SecurityUtils.install(sc);
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
        SecurityConfiguration sc =
                new SecurityConfiguration(
                        new Configuration(),
                        Collections.singletonList(
                                TestSecurityContextFactory.class.getCanonicalName()),
                        Collections.singletonList(
                                TestSecurityModuleFactory.class.getCanonicalName()));

        SecurityUtils.install(sc);
        assertThat(SecurityUtils.getInstalledContext().getClass())
                .isEqualTo(TestSecurityContextFactory.TestSecurityContext.class);

        SecurityUtils.uninstall();
        assertThat(SecurityUtils.getInstalledContext().getClass())
                .isEqualTo(NoOpSecurityContext.class);
    }

    @Test
    void testLinkageErrorShouldFallbackToSecond() throws Exception {
        Configuration testFlinkConf = new Configuration();

        testFlinkConf.set(
                SecurityOptions.SECURITY_CONTEXT_FACTORY_CLASSES,
                Lists.newArrayList(
                        LinkageErrorSecurityContextFactory.class.getCanonicalName(),
                        TestSecurityContextFactory.class.getCanonicalName()));

        SecurityConfiguration testSecurityConf = new SecurityConfiguration(testFlinkConf);

        SecurityUtils.install(testSecurityConf);
        assertThat(SecurityUtils.getInstalledContext().getClass())
                .isEqualTo(TestSecurityContextFactory.TestSecurityContext.class);

        SecurityUtils.uninstall();
        assertThat(SecurityUtils.getInstalledContext().getClass())
                .isEqualTo(NoOpSecurityContext.class);
    }

    /** Verify that we fall back to a second configuration if the first one is incompatible. */
    @Test
    void testSecurityContextShouldFallbackToSecond() throws Exception {
        Configuration testFlinkConf = new Configuration();

        testFlinkConf.set(
                SecurityOptions.SECURITY_CONTEXT_FACTORY_CLASSES,
                Lists.newArrayList(
                        IncompatibleTestSecurityContextFactory.class.getCanonicalName(),
                        TestSecurityContextFactory.class.getCanonicalName()));

        SecurityConfiguration testSecurityConf = new SecurityConfiguration(testFlinkConf);

        SecurityUtils.install(testSecurityConf);
        assertThat(SecurityUtils.getInstalledContext().getClass())
                .isEqualTo(TestSecurityContextFactory.TestSecurityContext.class);

        SecurityUtils.uninstall();
        assertThat(SecurityUtils.getInstalledContext().getClass())
                .isEqualTo(NoOpSecurityContext.class);
    }

    /** Verify that we pick the first valid security context. */
    @Test
    void testSecurityContextShouldPickFirstIfBothCompatible() throws Exception {
        Configuration testFlinkConf = new Configuration();

        testFlinkConf.set(
                SecurityOptions.SECURITY_CONTEXT_FACTORY_CLASSES,
                Lists.newArrayList(
                        AnotherCompatibleTestSecurityContextFactory.class.getCanonicalName(),
                        TestSecurityContextFactory.class.getCanonicalName()));

        SecurityConfiguration testSecurityConf = new SecurityConfiguration(testFlinkConf);

        SecurityUtils.install(testSecurityConf);
        assertThat(SecurityUtils.getInstalledContext().getClass())
                .isEqualTo(AnotherCompatibleTestSecurityContextFactory.TestSecurityContext.class);

        SecurityUtils.uninstall();
        assertThat(SecurityUtils.getInstalledContext().getClass())
                .isEqualTo(NoOpSecurityContext.class);

        testFlinkConf.set(
                SecurityOptions.SECURITY_CONTEXT_FACTORY_CLASSES,
                Lists.newArrayList(
                        TestSecurityContextFactory.class.getCanonicalName(),
                        AnotherCompatibleTestSecurityContextFactory.class.getCanonicalName()));

        testSecurityConf = new SecurityConfiguration(testFlinkConf);

        SecurityUtils.install(testSecurityConf);
        assertThat(SecurityUtils.getInstalledContext().getClass())
                .isEqualTo(TestSecurityContextFactory.TestSecurityContext.class);

        SecurityUtils.uninstall();
        assertThat(SecurityUtils.getInstalledContext().getClass())
                .isEqualTo(NoOpSecurityContext.class);
    }

    @Test
    void testSecurityFactoriesDefaultConfig() {
        Configuration testFlinkConf = new Configuration();
        SecurityConfiguration testSecurityConf = new SecurityConfiguration(testFlinkConf);
        // should load the default context security module factories.
        assertThat(testSecurityConf.getSecurityContextFactories()).hasSize(2);
        assertThat(testSecurityConf.getSecurityContextFactories().get(0))
                .isEqualTo(HadoopSecurityContextFactory.class.getCanonicalName());
        assertThat(testSecurityConf.getSecurityContextFactories().get(1))
                .isEqualTo(NoOpSecurityContextFactory.class.getCanonicalName());
        // should load 3 default security module factories.
        assertThat(testSecurityConf.getSecurityModuleFactories()).hasSize(3);
    }

    @Test
    void testSecurityFactoriesUserConfig() {
        Configuration testFlinkConf = new Configuration();

        testFlinkConf.set(
                SecurityOptions.SECURITY_CONTEXT_FACTORY_CLASSES,
                Collections.singletonList(TestSecurityContextFactory.class.getCanonicalName()));
        testFlinkConf.set(
                SecurityOptions.SECURITY_MODULE_FACTORY_CLASSES,
                Collections.singletonList(TestSecurityModuleFactory.class.getCanonicalName()));
        SecurityConfiguration testSecurityConf = new SecurityConfiguration(testFlinkConf);

        // should pick up the SecurityOptions to override default factories.
        assertThat(testSecurityConf.getSecurityContextFactories()).hasSize(1);
        assertThat(testSecurityConf.getSecurityContextFactories().get(0))
                .isEqualTo(TestSecurityContextFactory.class.getCanonicalName());
        assertThat(testSecurityConf.getSecurityModuleFactories()).hasSize(1);
        assertThat(testSecurityConf.getSecurityModuleFactories().get(0))
                .isEqualTo(TestSecurityModuleFactory.class.getCanonicalName());
    }

    @Test
    void testKerberosLoginContextParsing() {

        List<String> expectedLoginContexts = Arrays.asList("Foo bar", "Client");

        Configuration testFlinkConf;
        SecurityConfiguration testSecurityConf;

        // ------- no whitespaces

        testFlinkConf = new Configuration();
        testFlinkConf.setString(SecurityOptions.KERBEROS_LOGIN_CONTEXTS, "Foo bar,Client");
        testSecurityConf =
                new SecurityConfiguration(
                        testFlinkConf,
                        Collections.singletonList(
                                TestSecurityContextFactory.class.getCanonicalName()),
                        Collections.singletonList(
                                TestSecurityModuleFactory.class.getCanonicalName()));
        assertThat(testSecurityConf.getLoginContextNames()).isEqualTo(expectedLoginContexts);

        // ------- with whitespaces surrounding comma

        testFlinkConf = new Configuration();
        testFlinkConf.setString(SecurityOptions.KERBEROS_LOGIN_CONTEXTS, "Foo bar , Client");
        testSecurityConf =
                new SecurityConfiguration(
                        testFlinkConf,
                        Collections.singletonList(
                                TestSecurityContextFactory.class.getCanonicalName()),
                        Collections.singletonList(
                                TestSecurityModuleFactory.class.getCanonicalName()));
        assertThat(testSecurityConf.getLoginContextNames()).isEqualTo(expectedLoginContexts);

        // ------- leading / trailing whitespaces at start and end of list

        testFlinkConf = new Configuration();
        testFlinkConf.setString(SecurityOptions.KERBEROS_LOGIN_CONTEXTS, " Foo bar , Client ");
        testSecurityConf =
                new SecurityConfiguration(
                        testFlinkConf,
                        Collections.singletonList(
                                TestSecurityContextFactory.class.getCanonicalName()),
                        Collections.singletonList(
                                TestSecurityModuleFactory.class.getCanonicalName()));
        assertThat(testSecurityConf.getLoginContextNames()).isEqualTo(expectedLoginContexts);

        // ------- empty entries

        testFlinkConf = new Configuration();
        testFlinkConf.setString(SecurityOptions.KERBEROS_LOGIN_CONTEXTS, "Foo bar,,Client");
        testSecurityConf =
                new SecurityConfiguration(
                        testFlinkConf,
                        Collections.singletonList(
                                TestSecurityContextFactory.class.getCanonicalName()),
                        Collections.singletonList(
                                TestSecurityModuleFactory.class.getCanonicalName()));
        assertThat(testSecurityConf.getLoginContextNames()).isEqualTo(expectedLoginContexts);

        // ------- empty trailing String entries with whitespaces

        testFlinkConf = new Configuration();
        testFlinkConf.setString(SecurityOptions.KERBEROS_LOGIN_CONTEXTS, "Foo bar, ,, Client,");
        testSecurityConf =
                new SecurityConfiguration(
                        testFlinkConf,
                        Collections.singletonList(
                                TestSecurityContextFactory.class.getCanonicalName()),
                        Collections.singletonList(
                                TestSecurityModuleFactory.class.getCanonicalName()));
        assertThat(testSecurityConf.getLoginContextNames()).isEqualTo(expectedLoginContexts);
    }
}
