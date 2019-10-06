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

package org.apache.flink.hdfstests.userjar;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.TestLogger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URI;

import static org.junit.Assert.assertEquals;

/**
 * Tests for adding user jar file via HDFS.
 */
public class UserJarDfsTest extends TestLogger {

	private static final String localJarFile = "target/userjar-test-jar.jar";
	private static final String funcName = "org.apache.flink.test.userjar.MapFunc";

	@ClassRule
	public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

	@ClassRule
	public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE = new MiniClusterWithClientResource(
		new MiniClusterResourceConfiguration.Builder()
			.setNumberTaskManagers(1)
			.setNumberSlotsPerTaskManager(1)
			.build());

	private static MiniDFSCluster hdfsCluster;
	private static Configuration conf = new Configuration();

	private static Path dfsJarFile;

	@BeforeClass
	public static void setup() throws Exception {
		File dataDir = TEMP_FOLDER.newFolder();

		conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, dataDir.getAbsolutePath());
		MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
		hdfsCluster = builder.build();

		String hdfsURI = "hdfs://"
			+ NetUtils.hostAndPortToUrlString(hdfsCluster.getURI().getHost(), hdfsCluster.getNameNodePort())
			+ "/";

		FileSystem dfs = FileSystem.get(new URI(hdfsURI));
		dfsJarFile = writeFile(dfs, dfs.getHomeDirectory(), "userjar.jar");
	}

	private static Path writeFile(FileSystem dfs, Path rootDir, String fileName) throws IOException {
		Path file = new Path(rootDir, fileName);
		try (
			DataOutputStream outStream = new DataOutputStream(dfs.create(file,
				FileSystem.WriteMode.OVERWRITE))) {
			InputStream in = new FileInputStream(new File(localJarFile));
			byte[] b = new byte[4096];
			int len;
			while ((len = in.read(b)) != -1) {
				outStream.write(b, 0, len);
			}
		}
		return file;
	}

	@AfterClass
	public static void teardown() {
		hdfsCluster.shutdown();
	}

	@Test
	public void testUserJarViaDFS() throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		env.registerUserJarFile(dfsJarFile.toString());

		env.fromElements("1")
			.map(new UdfMapper())
			.addSink(new DiscardingSink<>());

		env.execute();
	}

	private static class UdfMapper extends RichMapFunction<String, String>{
		private Object mapper;
		private Method mapFunc;

		@Override
		public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
			ClassLoader loader = getRuntimeContext().getUserCodeClassLoader();
			Class clazz = Class.forName("org.apache.flink.hdfstests.userjar.MapFunc", false, loader);
			mapper = clazz.newInstance();
			mapFunc = clazz.getDeclaredMethod("eval", String.class);
		}

		@Override
		public String map(String value) throws Exception {
			String hello = (String) mapFunc.invoke(mapper, value);
			assertEquals("Hello Flink!", hello);
			return hello;
		}
	}
}
