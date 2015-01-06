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

import org.apache.flink.client.CliFrontend;
import org.apache.flink.client.FlinkYarnSessionCli;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * This base class allows to use the MiniYARNCluster.
 * The cluster is re-used for all tests.
 *
 * This class is located in a different package which is build after flink-dist. This way,
 * we can use the YARN uberjar of flink to start a Flink YARN session.
 */
public abstract class YarnTestBase {
	private static final Logger LOG = LoggerFactory.getLogger(YarnTestBase.class);

	private final static PrintStream originalStdout = System.out;
	private final static PrintStream originalStderr = System.err;


	// Temp directory which is deleted after the unit test.
	private static TemporaryFolder tmp = new TemporaryFolder();

	protected static MiniYARNCluster yarnCluster = null;

	protected static File flinkUberjar;
	private static File yarnConfFile;

	protected static final Configuration yarnConfiguration;
	static {
		yarnConfiguration = new YarnConfiguration();
		yarnConfiguration.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 512);
		yarnConfiguration.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, 4096); // 4096 is the available memory anyways
		yarnConfiguration.setBoolean(YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS, true);
		yarnConfiguration.setBoolean(YarnConfiguration.RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME, true);
		yarnConfiguration.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 2);
		yarnConfiguration.setInt(YarnConfiguration.RM_MAX_COMPLETED_APPLICATIONS, 2);
		yarnConfiguration.setInt(YarnConfiguration.DEBUG_NM_DELETE_DELAY_SEC, 3600);
		yarnConfiguration.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, true);
		yarnConfiguration.setInt(YarnConfiguration.NM_VCORES, 666); // memory is overwritten in the MiniYARNCluster.
		// so we have to change the number of cores for testing.
	}

	// This code is taken from: http://stackoverflow.com/a/7201825/568695
	// it changes the environment variables of this JVM. Use only for testing purposes!
	private static void setEnv(Map<String, String> newenv) {
		try {
			Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
			Field theEnvironmentField = processEnvironmentClass.getDeclaredField("theEnvironment");
			theEnvironmentField.setAccessible(true);
			Map<String, String> env = (Map<String, String>) theEnvironmentField.get(null);
			env.putAll(newenv);
			Field theCaseInsensitiveEnvironmentField = processEnvironmentClass.getDeclaredField("theCaseInsensitiveEnvironment");
			theCaseInsensitiveEnvironmentField.setAccessible(true);
			Map<String, String> cienv = (Map<String, String>) theCaseInsensitiveEnvironmentField.get(null);
			cienv.putAll(newenv);
		} catch (NoSuchFieldException e) {
			try {
				Class[] classes = Collections.class.getDeclaredClasses();
				Map<String, String> env = System.getenv();
				for (Class cl : classes) {
					if ("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
						Field field = cl.getDeclaredField("m");
						field.setAccessible(true);
						Object obj = field.get(env);
						Map<String, String> map = (Map<String, String>) obj;
						map.clear();
						map.putAll(newenv);
					}
				}
			} catch (Exception e2) {
				throw new RuntimeException(e2);
			}
		} catch (Exception e1) {
			throw new RuntimeException(e1);
		}
	}

	/**
	 * Sleep a bit between the tests (we are re-using the YARN cluster for the tests)
	 */
	@After
	public void sleep() {
		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {
			Assert.fail("Should not happen");
		}
	}

	@Before
	public void checkClusterEmpty() throws IOException, YarnException {
		YarnClient yarnClient = YarnClient.createYarnClient();
		yarnClient.init(yarnConfiguration);
		yarnClient.start();
		List<ApplicationReport> apps = yarnClient.getApplications();
		for(ApplicationReport app : apps) {
			if(app.getYarnApplicationState() != YarnApplicationState.FINISHED) {
				Assert.fail("There is at least one application on the cluster is not finished");
			}
		}
	}

	/**
	 * Locate a file or diretory directory
	 */
	public static File findFile(String startAt, FilenameFilter fnf) {
		File root = new File(startAt);
		String[] files = root.list();
		if(files == null) {
			return null;
		}
		for(String file : files) {

			File f = new File(startAt + File.separator + file);
			if(f.isDirectory()) {
				File r = findFile(f.getAbsolutePath(), fnf);
				if(r != null) {
					return r;
				}
			} else if (fnf.accept(f.getParentFile(), f.getName())) {
				return f;
			}

		}
		return null;
	}

	/**
	 * Filter to find root dir of the flink-yarn dist.
	 */
	public static class RootDirFilenameFilter implements FilenameFilter {
		@Override
		public boolean accept(File dir, String name) {
			return name.endsWith("yarn-uberjar.jar") && dir.toString().contains("/lib");
		}
	}
	public static class ContainsName implements FilenameFilter {
		private String name;
		private String excludeInPath = null;

		public ContainsName(String name) {
			this.name = name;
		}

		public ContainsName(String name, String excludeInPath) {
			this.name = name;
			this.excludeInPath = excludeInPath;
		}

		@Override
		public boolean accept(File dir, String name) {
			if(excludeInPath == null) {
				return name.contains(this.name);
			} else {
				return name.contains(this.name) && !dir.toString().contains(excludeInPath);
			}
		}
	}

	public static File writeYarnSiteConfigXML(Configuration yarnConf) throws IOException {
		tmp.create();
		File yarnSiteXML = new File(tmp.newFolder().getAbsolutePath() + "/yarn-site.xml");

		FileWriter writer = new FileWriter(yarnSiteXML);
		yarnConf.writeXml(writer);
		writer.flush();
		writer.close();
		return yarnSiteXML;
	}

	public static void startYARNWithConfig(Configuration conf) {
		flinkUberjar = findFile(".", new RootDirFilenameFilter());
		Assert.assertNotNull(flinkUberjar);
		String flinkDistRootDir = flinkUberjar.getParentFile().getParent();

		if (!flinkUberjar.exists()) {
			Assert.fail("Unable to locate yarn-uberjar.jar");
		}

		try {
			LOG.info("Starting up MiniYARN cluster");
			if (yarnCluster == null) {
				yarnCluster = new MiniYARNCluster(YarnTestBase.class.getName(), 2, 1, 1);

				yarnCluster.init(conf);
				yarnCluster.start();
			}

			Map<String, String> map = new HashMap<String, String>(System.getenv());
			File flinkConfFilePath = findFile(flinkDistRootDir, new ContainsName("flink-conf.yaml"));
			Assert.assertNotNull(flinkConfFilePath);
			map.put("FLINK_CONF_DIR", flinkConfFilePath.getParent());
			yarnConfFile = writeYarnSiteConfigXML(conf);
			map.put("YARN_CONF_DIR", yarnConfFile.getParentFile().getAbsolutePath());
			map.put("IN_TESTS", "yes we are in tests"); // see FlinkYarnClient() for more infos
			setEnv(map);

			Assert.assertTrue(yarnCluster.getServiceState() == Service.STATE.STARTED);
		} catch (Exception ex) {
			ex.printStackTrace();
			LOG.error("setup failure", ex);
			Assert.fail();
		}
	}

	/**
	 * Default @BeforeClass impl. Overwrite this for passing a different configuration
	 */
	@BeforeClass
	public static void setup() {
		startYARNWithConfig(yarnConfiguration);
	}

	// -------------------------- Runner -------------------------- //

	private static ByteArrayOutputStream outContent;
	private static ByteArrayOutputStream errContent;
	enum RunTypes {
		YARN_SESSION, CLI_FRONTEND
	}

	protected void runWithArgs(String[] args, String expect, RunTypes type) {
		LOG.info("Running with args {}", Arrays.toString(args));

		outContent = new ByteArrayOutputStream();
		errContent = new ByteArrayOutputStream();
		System.setOut(new PrintStream(outContent));
		System.setErr(new PrintStream(errContent));


		final int START_TIMEOUT_SECONDS = 60;

		Runner runner = new Runner(args, type);
		runner.start();

		boolean expectedStringSeen = false;
		for(int second = 0; second <  START_TIMEOUT_SECONDS; second++) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				Assert.fail("Interruption not expected");
			}
			// check output for correct TaskManager startup.
			if(outContent.toString().contains(expect)
					|| errContent.toString().contains(expect) ) {
				expectedStringSeen = true;
				LOG.info("Found expected output in redirected streams");
				// send "stop" command to command line interface
				runner.sendStop();
				// wait for the thread to stop
				try {
					runner.join(1000);
				} catch (InterruptedException e) {
					LOG.warn("Interrupted while stopping runner", e);
				}
				LOG.warn("stopped");
				break;
			}
			// check if thread died
			if(!runner.isAlive()) {
				sendOutput();
				Assert.fail("Runner thread died before the test was finished. Return value = "+runner.getReturnValue());
			}
		}

		sendOutput();
		Assert.assertTrue("During the timeout period of " + START_TIMEOUT_SECONDS + " seconds the " +
				"expected string did not show up", expectedStringSeen);
		LOG.info("Test was successful");
	}

	private static void sendOutput() {
		System.setOut(originalStdout);
		System.setErr(originalStderr);

		LOG.info("Sending stdout content through logger: \n\n{}\n\n", outContent.toString());
		LOG.info("Sending stderr content through logger: \n\n{}\n\n", errContent.toString());
	}

	public static class Runner extends Thread {
		private final String[] args;
		private int returnValue;
		private RunTypes type;
		private FlinkYarnSessionCli yCli;

		public Runner(String[] args, RunTypes type) {
			this.args = args;
			this.type = type;
		}

		public int getReturnValue() {
			return returnValue;
		}

		@Override
		public void run() {
			switch(type) {
				case YARN_SESSION:
					yCli = new FlinkYarnSessionCli("", "");
					returnValue = yCli.run(args);
					break;
				case CLI_FRONTEND:
					CliFrontend cli = new CliFrontend();
					returnValue = cli.parseParameters(args);
					break;
				default:
					throw new RuntimeException("Unknown type " + type);
			}

			if(returnValue != 0) {
				Assert.fail("The YARN session returned with non-null value="+returnValue);
			}
		}

		public void sendStop() {
			if(yCli != null) {
				yCli.stop();
			}
		}
	}

	// -------------------------- Tear down -------------------------- //

	@AfterClass
	public static void tearDown() {
		//shutdown YARN cluster
		if (yarnCluster != null) {
			LOG.info("shutdown MiniYarn cluster");
			yarnCluster.stop();
			yarnCluster = null;
		}
	}

}
