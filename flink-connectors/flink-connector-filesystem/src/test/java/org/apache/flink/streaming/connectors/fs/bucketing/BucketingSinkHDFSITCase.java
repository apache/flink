/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.fs.bucketing;

import org.apache.flink.util.NetUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;

/**
 * Fault tolerance test for {@link BucketingSink} on HDFS.
 */
public class BucketingSinkHDFSITCase extends BucketingSinkFaultToleranceBase {

	private static MiniDFSCluster hdfsCluster;

	@BeforeClass
	public static void createHDFS() throws IOException {
		Configuration conf = new Configuration();

		File dataDir = tempFolder.newFolder();

		conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, dataDir.getAbsolutePath());
		MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
		hdfsCluster = builder.build();

		dfs = hdfsCluster.getFileSystem();

		outPath = "hdfs://"
				+ NetUtils.hostAndPortToUrlString(hdfsCluster.getURI().getHost(), hdfsCluster.getNameNodePort())
				+ "/string-non-rolling-out";
	}

	@AfterClass
	public static void destroyHDFS() {
		if (hdfsCluster != null) {
			hdfsCluster.shutdown();
		}
	}
}
