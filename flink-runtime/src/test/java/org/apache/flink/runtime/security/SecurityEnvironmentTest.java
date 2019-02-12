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
import org.apache.flink.runtime.security.contexts.NoOpSecurityContext;
import org.apache.flink.runtime.security.factories.TestSecurityContextFactory;
import org.apache.flink.runtime.security.factories.TestSecurityModuleFactory;

import org.junit.AfterClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link SecurityEnvironment}.
 */
public class SecurityEnvironmentTest {

	@AfterClass
	public static void afterClass() {
		SecurityEnvironment.uninstall();
	}

	@Test
	public void testModuleInstall() throws Exception {
		SecurityConfiguration sc = new SecurityConfiguration(
			new Configuration(),
			TestSecurityContextFactory.class.getCanonicalName(),
			Collections.singletonList(TestSecurityModuleFactory.class.getCanonicalName()));

		SecurityEnvironment.install(sc);
		assertEquals(1, SecurityEnvironment.getInstalledModules().size());
		TestSecurityModuleFactory.TestSecurityModule testModule =
			(TestSecurityModuleFactory.TestSecurityModule) SecurityEnvironment.getInstalledModules().get(0);
		assertTrue(testModule.installed);

		SecurityEnvironment.uninstall();
		assertNull(SecurityEnvironment.getInstalledModules());
		assertFalse(testModule.installed);
	}

	@Test
	public void testSecurityContext() throws Exception {
		SecurityConfiguration sc = new SecurityConfiguration(
			new Configuration(),
			TestSecurityContextFactory.class.getCanonicalName(),
			Collections.singletonList(TestSecurityModuleFactory.class.getCanonicalName()));

		SecurityEnvironment.install(sc);
		assertEquals(TestSecurityContextFactory.TestSecurityContext.class, SecurityEnvironment.getInstalledContext().getClass());

		SecurityEnvironment.uninstall();
		assertEquals(NoOpSecurityContext.class, SecurityEnvironment.getInstalledContext().getClass());
	}

	@Test
	public void testKerberosLoginContextParsing() {

		List<String> expectedLoginContexts = Arrays.asList("Foo bar", "Client");

		Configuration testFlinkConf;
		SecurityConfiguration testSecurityConf;

		// ------- no whitespaces

		testFlinkConf = new Configuration();
		testFlinkConf.setString(SecurityOptions.KERBEROS_LOGIN_CONTEXTS, "Foo bar,Client");
		testSecurityConf = new SecurityConfiguration(
			testFlinkConf,
			TestSecurityContextFactory.class.getCanonicalName(),
			Collections.singletonList(TestSecurityModuleFactory.class.getCanonicalName()));
		assertEquals(expectedLoginContexts, testSecurityConf.getLoginContextNames());

		// ------- with whitespaces surrounding comma

		testFlinkConf = new Configuration();
		testFlinkConf.setString(SecurityOptions.KERBEROS_LOGIN_CONTEXTS, "Foo bar , Client");
		testSecurityConf = new SecurityConfiguration(
			testFlinkConf,
			TestSecurityContextFactory.class.getCanonicalName(),
			Collections.singletonList(TestSecurityModuleFactory.class.getCanonicalName()));
		assertEquals(expectedLoginContexts, testSecurityConf.getLoginContextNames());

		// ------- leading / trailing whitespaces at start and end of list

		testFlinkConf = new Configuration();
		testFlinkConf.setString(SecurityOptions.KERBEROS_LOGIN_CONTEXTS, " Foo bar , Client ");
		testSecurityConf = new SecurityConfiguration(
			testFlinkConf,
			TestSecurityContextFactory.class.getCanonicalName(),
			Collections.singletonList(TestSecurityModuleFactory.class.getCanonicalName()));
		assertEquals(expectedLoginContexts, testSecurityConf.getLoginContextNames());

		// ------- empty entries

		testFlinkConf = new Configuration();
		testFlinkConf.setString(SecurityOptions.KERBEROS_LOGIN_CONTEXTS, "Foo bar,,Client");
		testSecurityConf = new SecurityConfiguration(
			testFlinkConf,
			TestSecurityContextFactory.class.getCanonicalName(),
			Collections.singletonList(TestSecurityModuleFactory.class.getCanonicalName()));
		assertEquals(expectedLoginContexts, testSecurityConf.getLoginContextNames());

		// ------- empty trailing String entries with whitespaces

		testFlinkConf = new Configuration();
		testFlinkConf.setString(SecurityOptions.KERBEROS_LOGIN_CONTEXTS, "Foo bar, ,, Client,");
		testSecurityConf = new SecurityConfiguration(
			testFlinkConf,
			TestSecurityContextFactory.class.getCanonicalName(),
			Collections.singletonList(TestSecurityModuleFactory.class.getCanonicalName()));
		assertEquals(expectedLoginContexts, testSecurityConf.getLoginContextNames());
	}
}

