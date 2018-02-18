/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.	See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.	The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.	You may obtain a copy of the License at
 *
 *		 http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.security.modules.HadoopModule;
import org.apache.flink.runtime.security.modules.SecurityModule;

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Tests for {@link YarnTaskManagerRunnerFactory}.
 */
public class YarnTaskManagerRunnerFactoryTest {

	@Test
	public void testKerberosKeytabConfiguration() throws IOException {
		final String resourceDirPath =
				Paths.get("src", "test", "resources").toAbsolutePath().toString();
		final Map<String, String> envs = new HashMap<>();
		envs.put(YarnFlinkResourceManager.ENV_FLINK_CONTAINER_ID, "test_container_00001");
		envs.put(YarnConfigKeys.KEYTAB_PRINCIPAL, "testuser1@domain");
		envs.put(ApplicationConstants.Environment.PWD.key(), resourceDirPath);

		final YarnTaskManagerRunnerFactory.Runner tmRunner = YarnTaskManagerRunnerFactory.create(
			new String[]{"--configDir", resourceDirPath},
			YarnTaskManager.class,
			envs);

		final List<SecurityModule> modules = SecurityUtils.getInstalledModules();
		Optional<SecurityModule> moduleOpt =
			modules.stream().filter(s -> s instanceof HadoopModule).findFirst();
		if (moduleOpt.isPresent()) {
			HadoopModule hadoopModule = (HadoopModule) moduleOpt.get();
			assertEquals("testuser1@domain", hadoopModule.getSecurityConfig().getPrincipal());
			assertEquals(resourceDirPath + "/" + Utils.KEYTAB_FILE_NAME,
				hadoopModule.getSecurityConfig().getKeytab());
		} else {
			fail("Can not find HadoopModule!");
		}

		final Configuration configuration = tmRunner.getConfiguration();
		assertEquals(resourceDirPath + "/" + Utils.KEYTAB_FILE_NAME, configuration.getString(SecurityOptions.KERBEROS_LOGIN_KEYTAB));
		assertEquals("testuser1@domain", configuration.getString(SecurityOptions.KERBEROS_LOGIN_PRINCIPAL));
		assertEquals("test_container_00001", tmRunner.getResourceId().toString());
	}
}
