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

package org.apache.flink.runtime.fs.hdfs;

import org.apache.flink.core.fs.AbstractResumableWriterTest;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.util.HadoopUtils;
import org.apache.flink.util.OperatingSystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.File;

/**
 * Tests for the {@link HadoopRecoverableWriter}.
 */
public class HadoopResumableWriterTest extends AbstractResumableWriterTest {

	@ClassRule
	public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

	private static MiniDFSCluster hdfsCluster;

	/** The cached file system instance. */
	private static FileSystem fileSystem;

	private static Path basePath;

	@BeforeClass
	public static void testHadoopVersion() {
		Assume.assumeTrue(HadoopUtils.isMinHadoopVersion(2, 7));
	}

	@BeforeClass
	public static void verifyOS() {
		Assume.assumeTrue("HDFS cluster cannot be started on Windows without extensions.", !OperatingSystem.isWindows());
	}

	@BeforeClass
	public static void createHDFS() throws Exception {
		final File baseDir = TEMP_FOLDER.newFolder();

		final Configuration hdConf = new Configuration();
		hdConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());

		final MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(hdConf);
		hdfsCluster = builder.build();

		final org.apache.hadoop.fs.FileSystem hdfs = hdfsCluster.getFileSystem();

		fileSystem = new HadoopFileSystem(hdfs);
		basePath = new Path(hdfs.getUri() + "/tests");
	}

	@AfterClass
	public static void destroyHDFS() throws Exception {
		if (hdfsCluster != null) {
			hdfsCluster.getFileSystem().delete(new org.apache.hadoop.fs.Path(basePath.toUri()), true);
			hdfsCluster.shutdown();
		}
	}

	@Override
	public Path getBasePath() {
		return basePath;
	}

	@Override
	public FileSystem initializeFileSystem() {
		return fileSystem;
	}
}
