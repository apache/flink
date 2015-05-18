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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.CliFrontend;
import org.apache.flink.client.FlinkYarnSessionCli;
import org.apache.flink.runtime.yarn.AbstractFlinkYarnClient;
import org.apache.flink.test.util.TestBaseUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
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
		map.put("FLINK_CONF_DIR", tmpFolder.getAbsolutePath());
		TestBaseUtils.setEnv(map);
		Options options = new Options();
		FlinkYarnSessionCli cli = new FlinkYarnSessionCli("", "");
		cli.getYARNSessionCLIOptions(options);

		CommandLineParser parser = new PosixParser();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, new String[]{"run", "-j", "fake.jar", "-n", "15", "-D", "akka.ask.timeout=5 min"});
		} catch(Exception e) {
			e.printStackTrace();
			Assert.fail("Parsing failed with "+e.getMessage());
		}

		AbstractFlinkYarnClient flinkYarnClient = cli.createFlinkYarnClient(cmd);

		Assert.assertNotNull(flinkYarnClient);

		List<Tuple2<String, String>> dynProperties = CliFrontend.getDynamicProperties(flinkYarnClient.getDynamicPropertiesEncoded());
		Assert.assertEquals(1, dynProperties.size());
		Assert.assertEquals("akka.ask.timeout", dynProperties.get(0).f0);
		Assert.assertEquals("5 min", dynProperties.get(0).f1);
	}

}
