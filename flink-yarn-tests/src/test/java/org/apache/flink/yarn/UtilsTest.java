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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.util.TestLogger;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.util.ArrayList;
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
		File dir = YarnTestBase.findFile("..", new YarnTestBase.RootDirFilenameFilter());
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

	/**
	 * Remove 15% of the heap, at least 384MB.
	 *
	 */
	@Test
	public void testHeapCutoff() {
		Configuration conf = new Configuration();
		conf.setFloat(ResourceManagerOptions.CONTAINERIZED_HEAP_CUTOFF_RATIO, 0.15F);
		conf.setInteger(ResourceManagerOptions.CONTAINERIZED_HEAP_CUTOFF_MIN, 384);

		Assert.assertEquals(616, Utils.calculateHeapSize(1000, conf));
		Assert.assertEquals(8500, Utils.calculateHeapSize(10000, conf));

		// test different configuration
		Assert.assertEquals(3400, Utils.calculateHeapSize(4000, conf));

		conf.setString(ResourceManagerOptions.CONTAINERIZED_HEAP_CUTOFF_MIN.key(), "1000");
		conf.setString(ResourceManagerOptions.CONTAINERIZED_HEAP_CUTOFF_RATIO.key(), "0.1");
		Assert.assertEquals(3000, Utils.calculateHeapSize(4000, conf));

		conf.setString(ResourceManagerOptions.CONTAINERIZED_HEAP_CUTOFF_RATIO.key(), "0.5");
		Assert.assertEquals(2000, Utils.calculateHeapSize(4000, conf));

		conf.setString(ResourceManagerOptions.CONTAINERIZED_HEAP_CUTOFF_RATIO.key(), "1");
		Assert.assertEquals(0, Utils.calculateHeapSize(4000, conf));

		// test also deprecated keys
		conf = new Configuration();
		conf.setDouble(ConfigConstants.YARN_HEAP_CUTOFF_RATIO, 0.15);
		conf.setInteger(ConfigConstants.YARN_HEAP_CUTOFF_MIN, 384);

		Assert.assertEquals(616, Utils.calculateHeapSize(1000, conf));
		Assert.assertEquals(8500, Utils.calculateHeapSize(10000, conf));
	}

	@Test(expected = IllegalArgumentException.class)
	public void illegalArgument() {
		Configuration conf = new Configuration();
		conf.setString(ResourceManagerOptions.CONTAINERIZED_HEAP_CUTOFF_RATIO.key(), "1.1");
		Assert.assertEquals(0, Utils.calculateHeapSize(4000, conf));
	}

	@Test(expected = IllegalArgumentException.class)
	public void illegalArgumentNegative() {
		Configuration conf = new Configuration();
		conf.setString(ResourceManagerOptions.CONTAINERIZED_HEAP_CUTOFF_RATIO.key(), "-0.01");
		Assert.assertEquals(0, Utils.calculateHeapSize(4000, conf));
	}

	@Test(expected = IllegalArgumentException.class)
	public void tooMuchCutoff() {
		Configuration conf = new Configuration();
		conf.setString(ResourceManagerOptions.CONTAINERIZED_HEAP_CUTOFF_RATIO.key(), "6000");
		Assert.assertEquals(0, Utils.calculateHeapSize(4000, conf));
	}

	@Test
	public void testGetEnvironmentVariables() {
		Configuration testConf = new Configuration();
		testConf.setString("yarn.application-master.env.LD_LIBRARY_PATH", "/usr/lib/native");

		Map<String, String> res = Utils.getEnvironmentVariables("yarn.application-master.env.", testConf);

		Assert.assertEquals(1, res.size());
		Map.Entry<String, String> entry = res.entrySet().iterator().next();
		Assert.assertEquals("LD_LIBRARY_PATH", entry.getKey());
		Assert.assertEquals("/usr/lib/native", entry.getValue());
	}

	@Test
	public void testGetEnvironmentVariablesErroneous() {
		Configuration testConf = new Configuration();
		testConf.setString("yarn.application-master.env.", "/usr/lib/native");

		Map<String, String> res = Utils.getEnvironmentVariables("yarn.application-master.env.", testConf);

		Assert.assertEquals(0, res.size());
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
		env.put(YarnConfigKeys.FLINK_JAR_PATH, root.toURI().toString());
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

		ContaineredTaskManagerParameters tmParams = new ContaineredTaskManagerParameters(64,
			64, 16, 1, new HashMap<>(1));
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
				taskManagerConf, workingDirectory, taskManagerMainClass, LOG);
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

	//
	// --------------- Tools to test if a certain string has been logged with Log4j. -------------
	// See :  http://stackoverflow.com/questions/3717402/how-to-test-w-junit-that-warning-was-logged-w-log4j
	//
	private static TestAppender testAppender;
	public static void addTestAppender(Class target, Level level) {
		testAppender = new TestAppender();
		testAppender.setThreshold(level);
		org.apache.log4j.Logger lg = org.apache.log4j.Logger.getLogger(target);
		lg.setLevel(level);
		lg.addAppender(testAppender);
		//org.apache.log4j.Logger.getRootLogger().addAppender(testAppender);
	}

	public static void checkForLogString(String expected) {
		LoggingEvent found = getEventContainingString(expected);
		if (found != null) {
			LOG.info("Found expected string '" + expected + "' in log message " + found);
			return;
		}
		Assert.fail("Unable to find expected string '" + expected + "' in log messages");
	}

	public static LoggingEvent getEventContainingString(String expected) {
		if (testAppender == null) {
			throw new NullPointerException("Initialize test appender first");
		}
		LoggingEvent found = null;
		// make sure that different threads are not logging while the logs are checked
		synchronized (testAppender.events) {
			for (LoggingEvent event : testAppender.events) {
				if (event.getMessage().toString().contains(expected)) {
					found = event;
					break;
				}
			}
		}
		return found;
	}

	private static class TestAppender extends AppenderSkeleton {
		public final List<LoggingEvent> events = new ArrayList<>();

		public void close() {
		}

		public boolean requiresLayout() {
			return false;
		}

		@Override
		protected void append(LoggingEvent event) {
			synchronized (events){
				events.add(event);
			}
		}
	}
}

