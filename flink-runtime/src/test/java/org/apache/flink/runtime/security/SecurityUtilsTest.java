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

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.junit.AfterClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link SecurityUtils}. */
public class SecurityUtilsTest {

    @AfterClass
    public static void afterClass() {
        SecurityUtils.uninstall();
    }

    @Test
    public void testModuleInstall() throws Exception {
        SecurityConfiguration sc =
                new SecurityConfiguration(
                        new Configuration(),
                        Collections.singletonList(
                                TestSecurityContextFactory.class.getCanonicalName()),
                        Collections.singletonList(
                                TestSecurityModuleFactory.class.getCanonicalName()));

        SecurityUtils.install(sc);
        assertEquals(1, SecurityUtils.getInstalledModules().size());
        TestSecurityModuleFactory.TestSecurityModule testModule =
                (TestSecurityModuleFactory.TestSecurityModule)
                        SecurityUtils.getInstalledModules().get(0);
        assertTrue(testModule.installed);

        SecurityUtils.uninstall();
        assertNull(SecurityUtils.getInstalledModules());
        assertFalse(testModule.installed);
    }

    @Test
    public void testSecurityContext() throws Exception {
        SecurityConfiguration sc =
                new SecurityConfiguration(
                        new Configuration(),
                        Collections.singletonList(
                                TestSecurityContextFactory.class.getCanonicalName()),
                        Collections.singletonList(
                                TestSecurityModuleFactory.class.getCanonicalName()));

        SecurityUtils.install(sc);
        assertEquals(
                TestSecurityContextFactory.TestSecurityContext.class,
                SecurityUtils.getInstalledContext().getClass());

        SecurityUtils.uninstall();
        assertEquals(NoOpSecurityContext.class, SecurityUtils.getInstalledContext().getClass());
    }

    @Test
    public void testLinkageErrorShouldFallbackToSecond() throws Exception {
        Configuration testFlinkConf = new Configuration();

        testFlinkConf.set(
                SecurityOptions.SECURITY_CONTEXT_FACTORY_CLASSES,
                Lists.newArrayList(
                        LinkageErrorSecurityContextFactory.class.getCanonicalName(),
                        TestSecurityContextFactory.class.getCanonicalName()));

        SecurityConfiguration testSecurityConf = new SecurityConfiguration(testFlinkConf);

        SecurityUtils.install(testSecurityConf);
        assertEquals(
                TestSecurityContextFactory.TestSecurityContext.class,
                SecurityUtils.getInstalledContext().getClass());

        SecurityUtils.uninstall();
        assertEquals(NoOpSecurityContext.class, SecurityUtils.getInstalledContext().getClass());
    }

    /** Verify that we fall back to a second configuration if the first one is incompatible. */
    @Test
    public void testSecurityContextShouldFallbackToSecond() throws Exception {
        Configuration testFlinkConf = new Configuration();

        testFlinkConf.set(
                SecurityOptions.SECURITY_CONTEXT_FACTORY_CLASSES,
                Lists.newArrayList(
                        IncompatibleTestSecurityContextFactory.class.getCanonicalName(),
                        TestSecurityContextFactory.class.getCanonicalName()));

        SecurityConfiguration testSecurityConf = new SecurityConfiguration(testFlinkConf);

        SecurityUtils.install(testSecurityConf);
        assertEquals(
                TestSecurityContextFactory.TestSecurityContext.class,
                SecurityUtils.getInstalledContext().getClass());

        SecurityUtils.uninstall();
        assertEquals(NoOpSecurityContext.class, SecurityUtils.getInstalledContext().getClass());
    }

    /** Verify that we pick the first valid security context. */
    @Test
    public void testSecurityContextShouldPickFirstIfBothCompatible() throws Exception {
        Configuration testFlinkConf = new Configuration();

        testFlinkConf.set(
                SecurityOptions.SECURITY_CONTEXT_FACTORY_CLASSES,
                Lists.newArrayList(
                        AnotherCompatibleTestSecurityContextFactory.class.getCanonicalName(),
                        TestSecurityContextFactory.class.getCanonicalName()));

        SecurityConfiguration testSecurityConf = new SecurityConfiguration(testFlinkConf);

        SecurityUtils.install(testSecurityConf);
        assertEquals(
                AnotherCompatibleTestSecurityContextFactory.TestSecurityContext.class,
                SecurityUtils.getInstalledContext().getClass());

        SecurityUtils.uninstall();
        assertEquals(NoOpSecurityContext.class, SecurityUtils.getInstalledContext().getClass());

        testFlinkConf.set(
                SecurityOptions.SECURITY_CONTEXT_FACTORY_CLASSES,
                Lists.newArrayList(
                        TestSecurityContextFactory.class.getCanonicalName(),
                        AnotherCompatibleTestSecurityContextFactory.class.getCanonicalName()));

        testSecurityConf = new SecurityConfiguration(testFlinkConf);

        SecurityUtils.install(testSecurityConf);
        assertEquals(
                TestSecurityContextFactory.TestSecurityContext.class,
                SecurityUtils.getInstalledContext().getClass());

        SecurityUtils.uninstall();
        assertEquals(NoOpSecurityContext.class, SecurityUtils.getInstalledContext().getClass());
    }

    @Test
    public void testSecurityFactoriesDefaultConfig() {
        Configuration testFlinkConf = new Configuration();
        SecurityConfiguration testSecurityConf = new SecurityConfiguration(testFlinkConf);
        // should load the default context security module factories.
        assertEquals(2, testSecurityConf.getSecurityContextFactories().size());
        assertEquals(
                HadoopSecurityContextFactory.class.getCanonicalName(),
                testSecurityConf.getSecurityContextFactories().get(0));
        assertEquals(
                NoOpSecurityContextFactory.class.getCanonicalName(),
                testSecurityConf.getSecurityContextFactories().get(1));
        // should load 3 default security module factories.
        assertEquals(3, testSecurityConf.getSecurityModuleFactories().size());
    }

    @Test
    public void testSecurityFactoriesUserConfig() {
        Configuration testFlinkConf = new Configuration();

        testFlinkConf.set(
                SecurityOptions.SECURITY_CONTEXT_FACTORY_CLASSES,
                Collections.singletonList(TestSecurityContextFactory.class.getCanonicalName()));
        testFlinkConf.set(
                SecurityOptions.SECURITY_MODULE_FACTORY_CLASSES,
                Collections.singletonList(TestSecurityModuleFactory.class.getCanonicalName()));
        SecurityConfiguration testSecurityConf = new SecurityConfiguration(testFlinkConf);

        // should pick up the SecurityOptions to override default factories.
        assertEquals(1, testSecurityConf.getSecurityContextFactories().size());
        assertEquals(
                TestSecurityContextFactory.class.getCanonicalName(),
                testSecurityConf.getSecurityContextFactories().get(0));
        assertEquals(1, testSecurityConf.getSecurityModuleFactories().size());
        assertEquals(
                TestSecurityModuleFactory.class.getCanonicalName(),
                testSecurityConf.getSecurityModuleFactories().get(0));
    }

    @Test
    public void testKerberosLoginContextParsing() {

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
        assertEquals(expectedLoginContexts, testSecurityConf.getLoginContextNames());

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
        assertEquals(expectedLoginContexts, testSecurityConf.getLoginContextNames());

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
        assertEquals(expectedLoginContexts, testSecurityConf.getLoginContextNames());

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
        assertEquals(expectedLoginContexts, testSecurityConf.getLoginContextNames());

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
        assertEquals(expectedLoginContexts, testSecurityConf.getLoginContextNames());
    }
}
