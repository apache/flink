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

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.core.fs.Path;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Tests of FileInputFormat in HDFS.
 */
public class FileInputFormatTest {
	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	private String hdfsURI;
	private MiniDFSCluster hdfsCluster;
	private org.apache.hadoop.fs.Path hdPath;
	private org.apache.hadoop.fs.FileSystem hdfs;

	@Before
	public void createHDFS() {
		try {
			Configuration hdConf = new Configuration();

			File baseDir = temporaryFolder.newFolder();
			FileUtil.fullyDelete(baseDir);
			hdConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
			MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(hdConf);
			hdfsCluster = builder.build();

			hdfsURI = "hdfs://" + hdfsCluster.getURI().getHost() + ":" + hdfsCluster.getNameNodePort() + "/";

			hdPath = new org.apache.hadoop.fs.Path("/test");
			hdfs = hdPath.getFileSystem(hdConf);
			hdfs.mkdirs(hdPath);
			for (int i = 0; i < 100; i++) {
				FSDataOutputStream stream = hdfs.create(new org.apache.hadoop.fs.Path(hdPath, String.valueOf(i)));
				for (int j = 0; j < 10; j++) {
					stream.write(("Hello HDFS " + i + j + "\n").getBytes());
				}
				stream.close();
			}
		} catch (Throwable e) {
			e.printStackTrace();
			Assert.fail("Test failed " + e.getMessage());
		}
	}

	@After
	public void destroyHDFS() {
		try {
			hdfs.delete(hdPath, true);
			hdfsCluster.shutdown();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}

	@Test
	public void testCreateInputSplit() throws IOException {
		FileInputFormat<String> inputFormat = new FileInputFormat<String>() {
			@Override
			public boolean reachedEnd() {
				return false;
			}

			@Override
			public String nextRecord(String reuse) {
				return null;
			}
		};
		inputFormat.setFilePath(new Path(hdfsURI, "/test"));
		assertEquals(100, inputFormat.createInputSplits(3).length);
	}
}
