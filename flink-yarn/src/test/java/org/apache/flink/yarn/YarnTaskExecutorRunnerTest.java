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

package org.apache.flink.yarn;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.security.modules.HadoopModule;
import org.apache.flink.runtime.security.modules.SecurityModule;
import org.apache.flink.util.TestLogger;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.junit.Test;

import java.io.File;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link YarnTaskExecutorRunner}.
 */
public class YarnTaskExecutorRunnerTest extends TestLogger {

	@Test
	public void testDefaultKerberosKeytabConfiguration() throws Exception {
		final String resourceDirPath = Paths.get("src", "test", "resources").toAbsolutePath().toString();

		final Map<String, String> envs = new HashMap<>(2);
		envs.put(YarnConfigKeys.KEYTAB_PRINCIPAL, "testuser1@domain");
		envs.put(YarnConfigKeys.REMOTE_KEYTAB_PATH, resourceDirPath);
		// Local keytab path will be populated from default YarnConfigOptions.LOCALIZED_KEYTAB_PATH
		envs.put(YarnConfigKeys.LOCAL_KEYTAB_PATH, YarnConfigOptions.LOCALIZED_KEYTAB_PATH.defaultValue());

		Configuration configuration = new Configuration();
		YarnTaskExecutorRunner.setupAndModifyConfiguration(configuration, resourceDirPath, envs);

		// the SecurityContext is installed on TaskManager startup
		SecurityUtils.install(new SecurityConfiguration(configuration));

		final List<SecurityModule> modules = SecurityUtils.getInstalledModules();
		Optional<SecurityModule> moduleOpt = modules.stream().filter(module -> module instanceof HadoopModule).findFirst();

		if (moduleOpt.isPresent()) {
			HadoopModule hadoopModule = (HadoopModule) moduleOpt.get();
			assertThat(hadoopModule.getSecurityConfig().getPrincipal(), is("testuser1@domain"));
			assertThat(hadoopModule.getSecurityConfig().getKeytab(), is(new File(resourceDirPath, YarnConfigOptions.LOCALIZED_KEYTAB_PATH.defaultValue()).getAbsolutePath()));
		} else {
			fail("Can not find HadoopModule!");
		}

		assertThat(configuration.getString(SecurityOptions.KERBEROS_LOGIN_KEYTAB), is(new File(resourceDirPath, YarnConfigOptions.LOCALIZED_KEYTAB_PATH.defaultValue()).getAbsolutePath()));
		assertThat(configuration.getString(SecurityOptions.KERBEROS_LOGIN_PRINCIPAL), is("testuser1@domain"));
	}

	@Test
	public void testPreInstallKerberosKeytabConfiguration() throws Exception {
		final String resourceDirPath = Paths.get("src", "test", "resources").toAbsolutePath().toString();

		final Map<String, String> envs = new HashMap<>(2);
		envs.put(YarnConfigKeys.KEYTAB_PRINCIPAL, "testuser1@domain");
		// Try directly resolving local path when no remote keytab path is provided.
		envs.put(YarnConfigKeys.LOCAL_KEYTAB_PATH, "src/test/resources/krb5.keytab");

		Configuration configuration = new Configuration();
		YarnTaskExecutorRunner.setupAndModifyConfiguration(configuration, resourceDirPath, envs);

		// the SecurityContext is installed on TaskManager startup
		SecurityUtils.install(new SecurityConfiguration(configuration));

		final List<SecurityModule> modules = SecurityUtils.getInstalledModules();
		Optional<SecurityModule> moduleOpt = modules.stream().filter(module -> module instanceof HadoopModule).findFirst();

		if (moduleOpt.isPresent()) {
			HadoopModule hadoopModule = (HadoopModule) moduleOpt.get();
			assertThat(hadoopModule.getSecurityConfig().getPrincipal(), is("testuser1@domain"));
			// Using containString verification as the absolute path varies depending on runtime environment
			assertThat(hadoopModule.getSecurityConfig().getKeytab(), containsString("src/test/resources/krb5.keytab"));
		} else {
			fail("Can not find HadoopModule!");
		}

		assertThat(configuration.getString(SecurityOptions.KERBEROS_LOGIN_KEYTAB), containsString("src/test/resources/krb5.keytab"));
		assertThat(configuration.getString(SecurityOptions.KERBEROS_LOGIN_PRINCIPAL), is("testuser1@domain"));

	}
}
