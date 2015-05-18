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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

import org.apache.flink.runtime.state.FileStateHandle;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.runtime.state.StateHandleProvider;
import org.apache.flink.runtime.util.SerializedValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FileStateHandleTest {

	private String hdfsURI;
	private MiniDFSCluster hdfsCluster;
	private org.apache.hadoop.fs.Path hdPath;
	private org.apache.hadoop.fs.FileSystem hdfs;

	@Before
	public void createHDFS() {
		try {
			Configuration hdConf = new Configuration();

			File baseDir = new File("./target/hdfs/filestatehandletest").getAbsoluteFile();
			FileUtil.fullyDelete(baseDir);
			hdConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
			MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(hdConf);
			hdfsCluster = builder.build();

			hdfsURI = "hdfs://" + hdfsCluster.getURI().getHost() + ":"
					+ hdfsCluster.getNameNodePort() + "/";

			hdPath = new org.apache.hadoop.fs.Path("/StateHandleTest");
			hdfs = hdPath.getFileSystem(hdConf);
			hdfs.mkdirs(hdPath);

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
	public void testFileStateHandle() throws Exception {

		Serializable state = "state";

		// Create a state handle provider for the hdfs directory
		StateHandleProvider<Serializable> handleProvider = FileStateHandle.createProvider(hdfsURI
				+ hdPath);

		FileStateHandle handle = (FileStateHandle) handleProvider.createStateHandle(state);

		assertTrue(handle.stateFetched());

		// Serialize the handle so it writes the value to hdfs
		SerializedValue<StateHandle<Serializable>> serializedHandle = new SerializedValue<StateHandle<Serializable>>(
				handle);

		// Deserialize the handle and verify that the state is not fetched yet
		FileStateHandle deserializedHandle = (FileStateHandle) serializedHandle
				.deserializeValue(Thread.currentThread().getContextClassLoader());
		assertFalse(deserializedHandle.stateFetched());

		// Fetch the and compare with original
		assertEquals(state, deserializedHandle.getState());

		// Test whether discard removes the checkpoint file properly
		assertTrue(hdfs.listFiles(hdPath, true).hasNext());
		handle.discardState();
		assertFalse(hdfs.listFiles(hdPath, true).hasNext());

	}

}
