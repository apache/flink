/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.security.modules;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.security.SecurityConfiguration;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static org.apache.flink.runtime.security.modules.JaasModule.JAVA_SECURITY_AUTH_LOGIN_CONFIG;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link JaasModule}.
 */
public class JaasModuleTest {
	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	@Before
	public void setUp() throws IOException {
		// clear the property
		System.getProperties().remove(JAVA_SECURITY_AUTH_LOGIN_CONFIG);
		folder.create();
	}

	/**
	 * Test that the jaas config file is created in the working directory.
	 */
	@Test
	public void testJaasModuleFilePath() throws IOException {
		File file = folder.newFolder();
		String workingDir = file.toPath().toString();

		Configuration configuration = new Configuration();
		// set the string for CoreOptions.TMP_DIRS to mock the working directory.
		configuration.setString(CoreOptions.TMP_DIRS, workingDir);
		SecurityConfiguration sc = new SecurityConfiguration(configuration);
		JaasModule module = new JaasModule(sc);

		module.install();

		assertJaasFileLocateInRightDirectory(workingDir);
	}

	/**
	 * Test that the jaas file will be created in the directory specified by {@link CoreOptions#TMP_DIRS}'s default value
	 * if we do not manually specify it.
	 */
	@Test
	public void testCreateJaasModuleFileInTemporary() {
		Configuration configuration = new Configuration();
		SecurityConfiguration sc = new SecurityConfiguration(configuration);
		JaasModule module = new JaasModule(sc);

		module.install();

		assertJaasFileLocateInRightDirectory(CoreOptions.TMP_DIRS.defaultValue());
	}

	private void assertJaasFileLocateInRightDirectory(String directory) {
		assertTrue(System.getProperty(JAVA_SECURITY_AUTH_LOGIN_CONFIG).startsWith(directory));
	}
}

