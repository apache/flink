/**
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
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.yarn.util.TestUtils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for various utilities.
 */
public class UtilsTest extends TestLogger {
	private static final Logger LOG = LoggerFactory.getLogger(UtilsTest.class);

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testUberjarLocator() {
		File dir = TestUtils.findFile("..", new TestUtils.RootDirFilenameFilter());
		Assert.assertNotNull(dir);
		Assert.assertTrue(dir.getName().endsWith(".jar"));
		dir = dir.getParentFile().getParentFile(); // from uberjar to lib to root
		Assert.assertTrue(dir.exists());
		Assert.assertTrue(dir.isDirectory());
		List<String> files = Arrays.asList(dir.list());
		Assert.assertTrue(files.contains("lib"));
		Assert.assertTrue(files.contains("bin"));
		Assert.assertTrue(files.contains("conf"));
	}

	@Test
	public void testCreateTaskExecutorCredentials() throws Exception {
		File root = temporaryFolder.getRoot();
		File home = new File(root, "home");
		boolean created = home.mkdir();
		assertTrue(created);

		Configuration flinkConf = new Configuration();
		YarnConfiguration yarnConf = new YarnConfiguration();

		Map<String, String> env = new HashMap<>();
		env.put(YarnConfigKeys.ENV_APP_ID, "foo");
		env.put(YarnConfigKeys.ENV_CLIENT_HOME_DIR, home.getAbsolutePath());
		env.put(YarnConfigKeys.ENV_CLIENT_SHIP_FILES, "");
		env.put(YarnConfigKeys.ENV_FLINK_CLASSPATH, "");
		env.put(YarnConfigKeys.ENV_HADOOP_USER_NAME, "foo");
		env.put(YarnConfigKeys.FLINK_DIST_JAR, new YarnLocalResourceDescriptor(
			"flink.jar",
			new Path(root.toURI()),
			0,
			System.currentTimeMillis(),
			LocalResourceVisibility.APPLICATION,
			LocalResourceType.FILE).toString());
		env = Collections.unmodifiableMap(env);

		File credentialFile = temporaryFolder.newFile("container_tokens");
		final Text amRmTokenKind = AMRMTokenIdentifier.KIND_NAME;
		final Text hdfsDelegationTokenKind = new Text("HDFS_DELEGATION_TOKEN");
		final Text service = new Text("test-service");
		Credentials amCredentials = new Credentials();
		amCredentials.addToken(amRmTokenKind, new Token<>(new byte[4], new byte[4], amRmTokenKind, service));
		amCredentials.addToken(hdfsDelegationTokenKind, new Token<>(new byte[4], new byte[4],
			hdfsDelegationTokenKind, service));
		amCredentials.writeTokenStorageFile(new org.apache.hadoop.fs.Path(credentialFile.getAbsolutePath()), yarnConf);

		TaskExecutorProcessSpec spec = TaskExecutorProcessUtils
			.newProcessSpecBuilder(flinkConf)
			.withTotalProcessMemory(MemorySize.parse("1g"))
			.build();
		ContaineredTaskManagerParameters tmParams = new ContaineredTaskManagerParameters(spec, new HashMap<>(1));
		Configuration taskManagerConf = new Configuration();

		String workingDirectory = root.getAbsolutePath();
		Class<?> taskManagerMainClass = YarnTaskExecutorRunner.class;
		ContainerLaunchContext ctx;

		final Map<String, String> originalEnv = System.getenv();
		try {
			Map<String, String> systemEnv = new HashMap<>(originalEnv);
			systemEnv.put("HADOOP_TOKEN_FILE_LOCATION", credentialFile.getAbsolutePath());
			CommonTestUtils.setEnv(systemEnv);
			ctx = Utils.createTaskExecutorContext(flinkConf, yarnConf, env, tmParams,
				"", workingDirectory, taskManagerMainClass, LOG);
		} finally {
			CommonTestUtils.setEnv(originalEnv);
		}

		Credentials credentials = new Credentials();
		try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(ctx.getTokens().array()))) {
			credentials.readTokenStorageStream(dis);
		}
		Collection<Token<? extends TokenIdentifier>> tokens = credentials.getAllTokens();
		boolean hasHdfsDelegationToken = false;
		boolean hasAmRmToken = false;
		for (Token<? extends TokenIdentifier> token : tokens) {
			if (token.getKind().equals(amRmTokenKind)) {
				hasAmRmToken = true;
			} else if (token.getKind().equals(hdfsDelegationTokenKind)) {
				hasHdfsDelegationToken = true;
			}
		}
		assertTrue(hasHdfsDelegationToken);
		assertFalse(hasAmRmToken);
	}
}

