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

package org.apache.flink.kubernetes;

import org.apache.flink.client.cli.RunOptions;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.cli.KubernetesCustomCli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link KubernetesCustomCli}.
 */
public class KubernetesCustomCliTest {

	@Test
	public void testDynamicProperties() throws Exception {

		KubernetesCustomCli cli = new KubernetesCustomCli(new Configuration(), "");

		Options options = new Options();
		cli.addRunOptions(options);

		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = parser.parse(options, new String[]{"run", "-j", "fake.jar",
			"-D", "akka.ask.timeout=5 min", "-D", "env.java.opts=-DappName=foobar"});

		ClusterDescriptor kubernetesClusterDescriptor = cli.createClusterDescriptor(cmd);

		Assert.assertNotNull(kubernetesClusterDescriptor);

		Properties properties = cli.getDynamicProperties();
		assertEquals(2, properties.size());
		assertEquals("5 min", properties.get("akka.ask.timeout"));
		assertEquals("-DappName=foobar", properties.get("env.java.opts"));
	}

	@Test
	public void testCorrectSettingOfDetachedMode() throws Exception {
		String[] params = new String[] {"-d"};

		KubernetesCustomCli cli = new KubernetesCustomCli(new Configuration(), "");

		final CommandLine commandLine = cli.parseCommandLineOptions(params, true);

		final RunOptions runOptions = new RunOptions(commandLine);

		assertTrue(runOptions.getDetachedMode());
	}
}
