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
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.fs.hdfs.AbstractHadoopFileSystemITTest;
import org.apache.flink.testutils.oss.OSSTestCredentials;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;

import static junit.framework.TestCase.assertEquals;

/**
 * Unit tests for the OSS file system support via AliyunOSSFileSystem.
 * These tests do actually read from or write to OSS.
 */
public class HadoopOSSFileSystemITCase extends AbstractHadoopFileSystemITTest {

	private static final String TEST_DATA_DIR = "tests-" + UUID.randomUUID();

	@BeforeClass
	public static void setup() throws IOException {
		OSSTestCredentials.assumeCredentialsAvailable();

		final Configuration conf = new Configuration();
		conf.setString("fs.oss.endpoint", OSSTestCredentials.getOSSEndpoint());
		conf.setString("fs.oss.accessKeyId", OSSTestCredentials.getOSSAccessKey());
		conf.setString("fs.oss.accessKeySecret", OSSTestCredentials.getOSSSecretKey());
		FileSystem.initialize(conf);
		basePath = new Path(OSSTestCredentials.getTestBucketUri() + TEST_DATA_DIR);
		fs = basePath.getFileSystem();
		deadline = 0;
	}

	@Test
	public void testShadedConfigurations() {
		final Configuration conf = new Configuration();
		conf.setString("fs.oss.endpoint", OSSTestCredentials.getOSSEndpoint());
		conf.setString("fs.oss.accessKeyId", OSSTestCredentials.getOSSAccessKey());
		conf.setString("fs.oss.accessKeySecret", OSSTestCredentials.getOSSSecretKey());
		conf.setString("fs.oss.credentials.provider", "org.apache.hadoop.fs.aliyun.oss.AliyunCredentialsProvider");

		OSSFileSystemFactory ossfsFactory = new OSSFileSystemFactory();
		ossfsFactory.configure(conf);
		org.apache.hadoop.conf.Configuration configuration = ossfsFactory.getHadoopConfiguration();
		// shaded
		assertEquals("org.apache.flink.fs.osshadoop.shaded.org.apache.hadoop.fs.aliyun.oss.AliyunCredentialsProvider",
			configuration.get("fs.oss.credentials.provider"));
		// should not shaded
		assertEquals(OSSTestCredentials.getOSSEndpoint(), configuration.get("fs.oss.endpoint"));
		assertEquals(OSSTestCredentials.getOSSAccessKey(), configuration.get("fs.oss.accessKeyId"));
		assertEquals(OSSTestCredentials.getOSSSecretKey(), configuration.get("fs.oss.accessKeySecret"));
	}
}
