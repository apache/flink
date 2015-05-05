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
package org.apache.flink.tachyon;

import org.apache.commons.io.IOUtils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironmentFactory;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.examples.java.wordcount.WordCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import tachyon.client.InStream;
import tachyon.client.OutStream;
import tachyon.client.ReadType;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;
import tachyon.master.LocalTachyonCluster;

import java.io.File;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.net.URL;

public class TachyonFileSystemWrapperTest {
	private static final long TACHYON_WORKER_CAPACITY = 1024 * 1024 * 32;
	private static final String TACHYON_TEST_IN_FILE_NAME = "tachyontest";
	private static final String TACHYON_TEST_OUT_FILE_NAME = "result";
	private static final Path HADOOP_CONFIG_PATH;

	static {
		URL resource = TachyonFileSystemWrapperTest.class.getResource("/tachyonHadoopConf.xml");
		File file = null;
		try {
			file = new File(resource.toURI());
		} catch (URISyntaxException e) {
			throw new RuntimeException("Unable to load req. res", e);
		}
		if(!file.exists()) {
			throw new RuntimeException("Unable to load required resource");
		}
		HADOOP_CONFIG_PATH = new Path(file.getAbsolutePath());
	}

	private LocalTachyonCluster cluster;
	private TachyonFS client;
	private String input;
	private String output;

	@Before
	public void startTachyon() {
		try {
			cluster = new LocalTachyonCluster(TACHYON_WORKER_CAPACITY);
			cluster.start();
			client = cluster.getClient();
			int id = client.createFile("/" + TACHYON_TEST_IN_FILE_NAME, 1024 * 32);
			Assert.assertNotEquals("Unable to create file", -1, id);

			TachyonFile testFile = client.getFile(id);
			Assert.assertNotNull(testFile);


			OutStream outStream = testFile.getOutStream(WriteType.MUST_CACHE);
			for(int i = 0; i < 10; i++) {
				outStream.write("Hello Tachyon\n".getBytes());
			}
			outStream.close();
			final String tachyonBase = "tachyon://" + cluster.getMasterHostname() + ":" + cluster.getMasterPort();
			input = tachyonBase + "/" + TACHYON_TEST_IN_FILE_NAME;
			output = tachyonBase + "/" + TACHYON_TEST_OUT_FILE_NAME;

		} catch(Exception e) {
			e.printStackTrace();
			Assert.fail("Test preparation failed with exception: "+e.getMessage());
		}
	}

	@After
	public void stopTachyon() {
		try {
			cluster.stop();
		} catch(Exception e) {
			e.printStackTrace();
			Assert.fail("Test teardown failed with exception: "+e.getMessage());
		}
	}
	// Verify that Hadoop's FileSystem can load the TFS (Tachyon File System)
	@Test
	public void testHadoopLoadability() {
		try {
			Path tPath = new Path(input);
			Configuration conf = new Configuration();
			conf.addResource(HADOOP_CONFIG_PATH);
			Assert.assertEquals("tachyon.hadoop.TFS", conf.get("fs.tachyon.impl", null));
			tPath.getFileSystem(conf);
		} catch(Exception e) {
			e.printStackTrace();
			Assert.fail("Test failed with exception: "+e.getMessage());
		}
	}


	@Test
	public void testTachyon() {
		try {
			org.apache.flink.configuration.Configuration addHDConfToFlinkConf = new org.apache.flink.configuration.Configuration();
			addHDConfToFlinkConf.setString(ConfigConstants.HDFS_DEFAULT_CONFIG, HADOOP_CONFIG_PATH.toString());
			GlobalConfiguration.includeConfiguration(addHDConfToFlinkConf);

			new DopOneTestEnvironment(); // initialize parallelism one

			WordCount.main(new String[]{input, output});

			// verify result
			TachyonFile resultFile = client.getFile("/" + TACHYON_TEST_OUT_FILE_NAME);
			Assert.assertNotNull("Result file has not been created", resultFile);
			InStream inStream = resultFile.getInStream(ReadType.CACHE);
			Assert.assertNotNull("Result file has not been created", inStream);
			StringWriter writer = new StringWriter();
			IOUtils.copy(inStream, writer);
			String resultString = writer.toString();

			Assert.assertEquals("(hello,10)\n" +
					"(tachyon,10)\n", resultString);

		} catch(Exception e) {
			e.printStackTrace();
			Assert.fail("Test failed with exception: "+e.getMessage());
		}
	}

	// package visible
	static final class DopOneTestEnvironment extends LocalEnvironment {
	 	static {
    		initializeContextEnvironment(new ExecutionEnvironmentFactory() {
				@Override
				public ExecutionEnvironment createExecutionEnvironment() {
					LocalEnvironment le = new LocalEnvironment();
					le.setParallelism(1);
					return le;
				}
			});
		}
	}

}
