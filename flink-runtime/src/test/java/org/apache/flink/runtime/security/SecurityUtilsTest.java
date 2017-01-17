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
import org.apache.flink.runtime.security.modules.SecurityModule;
import org.junit.AfterClass;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.*;

/**
 * Tests for the {@link SecurityUtils}.
 */
public class SecurityUtilsTest {

	static class TestSecurityModule implements SecurityModule {
		boolean installed;

		@Override
		public void install(SecurityUtils.SecurityConfiguration configuration) throws SecurityInstallException {
			installed = true;
		}

		@Override
		public void uninstall() throws SecurityInstallException {
			installed = false;
		}
	}

	@AfterClass
	public static void afterClass() {
		SecurityUtils.uninstall();
	}

	@Test
	public void testModuleInstall() throws Exception {
		SecurityUtils.SecurityConfiguration sc = new SecurityUtils.SecurityConfiguration(
			new Configuration(), new org.apache.hadoop.conf.Configuration(),
			Collections.singletonList(TestSecurityModule.class));

		SecurityUtils.install(sc);
		assertEquals(1, SecurityUtils.getInstalledModules().size());
		TestSecurityModule testModule = (TestSecurityModule) SecurityUtils.getInstalledModules().get(0);
		assertTrue(testModule.installed);

		SecurityUtils.uninstall();
		assertNull(SecurityUtils.getInstalledModules());
		assertFalse(testModule.installed);
	}

	@Test
	public void testSecurityContext() throws Exception {
		SecurityUtils.SecurityConfiguration sc = new SecurityUtils.SecurityConfiguration(
			new Configuration(), new org.apache.hadoop.conf.Configuration(),
			Collections.singletonList(TestSecurityModule.class));

		SecurityUtils.install(sc);
		assertEquals(HadoopSecurityContext.class, SecurityUtils.getInstalledContext().getClass());

		SecurityUtils.uninstall();
		assertEquals(NoOpSecurityContext.class, SecurityUtils.getInstalledContext().getClass());
	}
}
