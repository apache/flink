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

import akka.actor.ActorSystem;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.client.CliFrontend;
import org.apache.flink.client.cli.CliFrontendParser;
import org.apache.flink.client.cli.RunOptions;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.yarn.cli.FlinkYarnSessionCli;
import org.apache.flink.test.util.TestBaseUtils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

public class FlinkYarnSessionCliTest {

	@Rule
	public TemporaryFolder tmp = new TemporaryFolder();

	@Test
	public void testDynamicProperties() throws IOException {

		Map<String, String> map = new HashMap<String, String>(System.getenv());
		File tmpFolder = tmp.newFolder();
		File fakeConf = new File(tmpFolder, "flink-conf.yaml");
		fakeConf.createNewFile();
		map.put(ConfigConstants.ENV_FLINK_CONF_DIR, tmpFolder.getAbsolutePath());
		TestBaseUtils.setEnv(map);
		FlinkYarnSessionCli cli = new FlinkYarnSessionCli("", "", false);
		Options options = new Options();
		cli.addGeneralOptions(options);
		cli.addRunOptions(options);

		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, new String[]{"run", "-j", "fake.jar", "-n", "15", "-D", "akka.ask.timeout=5 min"});
		} catch(Exception e) {
			e.printStackTrace();
			Assert.fail("Parsing failed with " + e.getMessage());
		}

		AbstractYarnClusterDescriptor flinkYarnDescriptor = cli.createDescriptor(null, cmd);

		Assert.assertNotNull(flinkYarnDescriptor);

		Map<String, String> dynProperties =
			FlinkYarnSessionCli.getDynamicProperties(flinkYarnDescriptor.getDynamicPropertiesEncoded());
		Assert.assertEquals(1, dynProperties.size());
		Assert.assertEquals("5 min", dynProperties.get("akka.ask.timeout"));
	}

	@Test
	public void testNotEnoughTaskSlots() throws Exception {

		File confFile = tmp.newFile("flink-conf.yaml");
		File jarFile = tmp.newFile("test.jar");
		new CliFrontend(tmp.getRoot().getAbsolutePath());

		String[] params =
			new String[] {"-yn", "2", "-ys", "3", "-p", "7", jarFile.getAbsolutePath()};

		RunOptions runOptions = CliFrontendParser.parseRunCommand(params);

		FlinkYarnSessionCli yarnCLI = new TestCLI("y", "yarn");

		AbstractYarnClusterDescriptor descriptor = yarnCLI.createDescriptor("", runOptions.getCommandLine());

		// each task manager has 3 slots but the parallelism is 7. Thus the slots should be increased.
		Assert.assertEquals(4, descriptor.getTaskManagerSlots());
		Assert.assertEquals(2, descriptor.getTaskManagerCount());
	}

	@Test
	public void testCorrectSettingOfMaxSlots() throws Exception {

		File confFile = tmp.newFile("flink-conf.yaml");
		File jarFile = tmp.newFile("test.jar");
		new CliFrontend(tmp.getRoot().getAbsolutePath());

		String[] params =
			new String[] {"-yn", "2", "-ys", "3", jarFile.getAbsolutePath()};

		RunOptions runOptions = CliFrontendParser.parseRunCommand(params);

		FlinkYarnSessionCli yarnCLI = new TestCLI("y", "yarn");

		AbstractYarnClusterDescriptor descriptor = yarnCLI.createDescriptor("", runOptions.getCommandLine());

		// each task manager has 3 slots but the parallelism is 7. Thus the slots should be increased.
		Assert.assertEquals(3, descriptor.getTaskManagerSlots());
		Assert.assertEquals(2, descriptor.getTaskManagerCount());

		Configuration config = new Configuration();
		CliFrontend.setJobManagerAddressInConfig(config, new InetSocketAddress("test", 9000));
		ClusterClient client = new TestingYarnClusterClient(descriptor, config);
		Assert.assertEquals(6, client.getMaxSlots());
	}

	@Test
	public void testZookeeperNamespaceProperty() throws Exception {

		File confFile = tmp.newFile("flink-conf.yaml");
		File jarFile = tmp.newFile("test.jar");
		new CliFrontend(tmp.getRoot().getAbsolutePath());

		String zkNamespaceCliInput = "flink_test_namespace";

		String[] params =
				new String[] {"-yn", "2", "-yz", zkNamespaceCliInput, jarFile.getAbsolutePath()};

		RunOptions runOptions = CliFrontendParser.parseRunCommand(params);

		FlinkYarnSessionCli yarnCLI = new TestCLI("y", "yarn");
		AbstractYarnClusterDescriptor descriptor = yarnCLI.createDescriptor("", runOptions.getCommandLine());

		Assert.assertEquals(zkNamespaceCliInput, descriptor.getZookeeperNamespace());
	}

	private static class TestCLI extends FlinkYarnSessionCli {

		public TestCLI(String shortPrefix, String longPrefix) {
			super(shortPrefix, longPrefix);
		}

		private static class JarAgnosticClusterDescriptor extends YarnClusterDescriptor {
			@Override
			public void setLocalJarPath(Path localJarPath) {
				// add nothing
			}
		}

		@Override
		protected AbstractYarnClusterDescriptor getClusterDescriptor() {
			return new JarAgnosticClusterDescriptor();
		}
	}

	private static class TestingYarnClusterClient extends YarnClusterClient {

		public TestingYarnClusterClient(AbstractYarnClusterDescriptor descriptor, Configuration config) throws IOException, YarnException {
			super(descriptor,
				Mockito.mock(YarnClient.class),
				Mockito.mock(ApplicationReport.class),
				config,
				new Path("/tmp"), false);
		}
	}
}
