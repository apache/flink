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

package org.apache.flink.hdfstests;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class FileUtilsTest {
	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	private static File TEMP_DIR;

	private static String HDFS_ROOT_URI;

	private static MiniDFSCluster HDFS_CLUSTER;

	private static FileSystem FS;

	// ------------------------------------------------------------------------
	//  startup / shutdown
	// ------------------------------------------------------------------------

	@BeforeClass
	public static void createHDFS() throws IOException, URISyntaxException {
		TEMP_DIR = temporaryFolder.newFolder();

		Configuration hdConf = new Configuration();
		hdConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, TEMP_DIR.getAbsolutePath());
		MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(hdConf);
		HDFS_CLUSTER = builder.build();
		HDFS_ROOT_URI = "hdfs://" + HDFS_CLUSTER.getURI().getHost() + ":" + HDFS_CLUSTER.getNameNodePort() + "/";

		FS = FileSystem.get(new URI(HDFS_ROOT_URI));
	}

	@AfterClass
	public static void destroyHDFS() {
		try {
			HDFS_CLUSTER.shutdown();
		}
		catch (Exception ignored) {}
	}

	@Test
	public void testCompareFs() throws IOException {
		File localDir = temporaryFolder.newFolder();
		Assert.assertFalse(FileUtils.compareFs(new Path(localDir.toURI()).getFileSystem(), FS));
	}

	@Test
	public void testLocalizeFile() throws IOException {
		Path localDir = new Path(temporaryFolder.newFolder().toURI());
		Path localDir2 = new Path(FS.getHomeDirectory(), "localDir");

		Path remoteFile = new Path(FS.getHomeDirectory(), "remoteFile");
		FSDataOutputStream outputStream = FS.create(remoteFile, FileSystem.WriteMode.OVERWRITE);
		outputStream.write(1);
		outputStream.close();


		Path result = new Path(FileUtils.localizeRemoteFile(localDir, remoteFile.toUri()));
		Assert.assertTrue(FileUtils.compareFs(result.getFileSystem(), localDir.getFileSystem()));
		FSDataInputStream inputStream = result.getFileSystem().open(result);
		Assert.assertEquals(1, inputStream.read());
		Assert.assertEquals(-1, inputStream.read());
		inputStream.close();

		result = new Path(FileUtils.localizeRemoteFile(localDir2, remoteFile.toUri()));
		Assert.assertTrue(FileUtils.compareFs(result.getFileSystem(), localDir2.getFileSystem()));
		inputStream = result.getFileSystem().open(result);
		Assert.assertEquals(1, inputStream.read());
		Assert.assertEquals(-1, inputStream.read());
		inputStream.close();
	}
}
