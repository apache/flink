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

package org.apache.flink.fs.osshadoop;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.TestLogger;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

/**
 * Unit tests for the OSS file system support via AliyunOSSFileSystem.
 * These tests do actually read from or write to OSS.
 */
public class HadoopOSSFileSystemITCase extends TestLogger {

	private static final String ENDPOINT = System.getenv("ARTIFACTS_OSS_ENDPOINT");
	private static final String BUCKET = System.getenv("ARTIFACTS_OSS_BUCKET");
	private static final String TEST_DATA_DIR = "tests-" + UUID.randomUUID();
	private static final String ACCESS_KEY = System.getenv("ARTIFACTS_OSS_ACCESS_KEY");
	private static final String SECRET_KEY = System.getenv("ARTIFACTS_OSS_SECRET_KEY");

	@BeforeClass
	public static void checkIfCredentialsArePresent() {
		Assume.assumeTrue("Aliyun OSS endpoint not configured, skipping test...", ENDPOINT != null);
		Assume.assumeTrue("Aliyun OSS bucket not configured, skipping test...", BUCKET != null);
		Assume.assumeTrue("Aliyun OSS access key not configured, skipping test...", ACCESS_KEY != null);
		Assume.assumeTrue("Aliyun OSS secret key not configured, skipping test...", SECRET_KEY != null);
	}

	@Test
	public void testReadAndWrite() throws Exception {
		final Configuration conf = new Configuration();
		conf.setString("fs.oss.endpoint", ENDPOINT);
		conf.setString("fs.oss.accessKeyId", ACCESS_KEY);
		conf.setString("fs.oss.accessKeySecret", SECRET_KEY);
		final String testLine = "Aliyun OSS";

		FileSystem.initialize(conf);
		final Path path = new Path("oss://" + BUCKET + '/' + TEST_DATA_DIR);
		final FileSystem fs = path.getFileSystem();
		try {
			for (int i = 0; i < 10; ++i) {
				final Path file = new Path(path.getPath() + "/test.data." + i);
				try (FSDataOutputStream out = fs.create(file, FileSystem.WriteMode.OVERWRITE)) {
					try (OutputStreamWriter writer = new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
						writer.write(testLine);
					}
				}
				try (FSDataInputStream in = fs.open(file);
					InputStreamReader ir = new InputStreamReader(in, StandardCharsets.UTF_8);
					BufferedReader reader = new BufferedReader(ir)) {
					String line = reader.readLine();
					assertEquals(testLine, line);
				}
			}
			assertTrue(fs.exists(path));
			assertEquals(10, fs.listStatus(path).length);
		} finally {
			fs.delete(path, true);
		}
	}

	@Test
	public void testShadedConfigurations() {
		final Configuration conf = new Configuration();
		conf.setString("fs.oss.endpoint", ENDPOINT);
		conf.setString("fs.oss.accessKeyId", ACCESS_KEY);
		conf.setString("fs.oss.accessKeySecret", SECRET_KEY);
		conf.setString("fs.oss.credentials.provider", "org.apache.hadoop.fs.aliyun.oss.AliyunCredentialsProvider");

		OSSFileSystemFactory ossfsFactory = new OSSFileSystemFactory();
		ossfsFactory.configure(conf);
		org.apache.hadoop.conf.Configuration configuration = ossfsFactory.getHadoopConfiguration();
		// shaded
		assertEquals("org.apache.flink.fs.shaded.hadoop3.org.apache.hadoop.fs.aliyun.oss.AliyunCredentialsProvider",
			configuration.get("fs.oss.credentials.provider"));
		// should not shaded
		assertEquals(ENDPOINT, configuration.get("fs.oss.endpoint"));
		assertEquals(ACCESS_KEY, configuration.get("fs.oss.accessKeyId"));
		assertEquals(SECRET_KEY, configuration.get("fs.oss.accessKeySecret"));
	}
}
