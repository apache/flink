/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.localDistributed;

import org.apache.flink.client.program.Client;
import org.apache.flink.client.program.JobWithJars;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.util.testjar.WordCountForTest;
import org.apache.flink.test.testdata.WordCountData;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class RemoteStreamExecutionTest {

	@Test
	public void testEverything() {
		ForkableFlinkMiniCluster cluster = null;

		File doc = null;
		File outFile = null;
		try {
			// set up the files
			doc = File.createTempFile("wc_input", ".in");
			outFile = File.createTempFile("wc_result", ".out");
			outFile.delete();

			FileWriter fwDoc = new FileWriter(doc);
			fwDoc.write(WordCountData.TEXT);
			fwDoc.close();

			fwDoc.close();

			String jarPath = "target/maven-test-jar.jar";
			File jarFile = new File(jarPath);
			List<File> jarFiles = new ArrayList<File>();
			jarFiles.add(jarFile);

			// run Word Count
			Configuration config = new Configuration();
			config.setInteger(ConfigConstants.LOCAL_INSTANCE_MANAGER_NUMBER_TASK_MANAGER, 2);
			config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 2);
			cluster = new ForkableFlinkMiniCluster(config, false);

			Client client = new Client(new InetSocketAddress("localhost", cluster.getJobManagerRPCPort()), config,
				JobWithJars.buildUserCodeClassLoader(jarFiles, JobWithJars.class.getClassLoader()));

			client.run(new WordCountForTest().getJobGraph(new String[] {
				doc.toPath().toString(),
				outFile.toPath().toString()}),
				true);

		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
		finally {
			if (doc != null) {
				doc.delete();
			}
			if (outFile != null) {
				outFile.delete();
			}

			try {
				if(cluster != null) {
					cluster.stop();
				}
			} catch (Exception e) {
				e.printStackTrace();
				Assert.fail(e.getMessage());
			}
		}
	}
}
