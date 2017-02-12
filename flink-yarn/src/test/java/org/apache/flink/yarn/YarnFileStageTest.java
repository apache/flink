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
package org.apache.flink.yarn;

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.streaming.util.HDFSCopyFromLocal;
import org.apache.flink.streaming.util.HDFSCopyToLocal;
import org.apache.flink.util.OperatingSystem;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class YarnFileStageTest {

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Before
	public void checkOperatingSystem() {
		Assume.assumeTrue("This test can't run successfully on Windows.", !OperatingSystem.isWindows());
	}

	/**
	 * This test verifies that nested directories are properly copied.
	 */
	@Test
	public void testCopyFromLocalRecursive() throws Exception {

		FileSystem fs = FileSystem.get(HadoopFileSystem.getHadoopConfiguration());
		File rootDir = tempFolder.newFolder();
		File nestedDir = new File(rootDir,"nested");
		nestedDir.mkdir();

		Map<String,File>  copyFiles = new HashMap<String,File>();

		copyFiles.put("1",new File(rootDir, "1"));
		copyFiles.put("2",new File(rootDir, "2"));
		copyFiles.put("3",new File(nestedDir, "3"));

		for (File file : copyFiles.values()) {
			try (DataOutputStream out = new DataOutputStream(new FileOutputStream(file))) {
				out.writeUTF("Hello there, " + file.getName());
			}
		}
		//add root and nested dirs to expected output
		copyFiles.put(rootDir.getName(),rootDir);
		copyFiles.put("nested",nestedDir);

		assertEquals(5,copyFiles.size());

		//Test for copy to unspecified target directory
		File copyDirU = tempFolder.newFolder();
		HDFSCopyFromLocal.copyFromLocal(
				rootDir,
				new Path("file://" + copyDirU.getAbsolutePath()).toUri());

		//Test for copy to specified target directory
		File copyDirQ = tempFolder.newFolder();
		HDFSCopyFromLocal.copyFromLocal(
				rootDir,
				new Path("file://" + copyDirQ.getAbsolutePath() + "/" + rootDir.getName()).toUri());

		//We only want to verify intended files, not CRC shadow files.
		FilenameFilter noCrc = new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return !name.endsWith(".crc");
			}
		};

		File dirCopyU = copyDirU.listFiles(noCrc)[0];
		File dirCopyQ = copyDirQ.listFiles(noCrc)[0];

		assertEquals(dirCopyU.getName(),dirCopyQ.getName());

		assertEquals(rootDir.getName(),dirCopyU.getName());
		assertNotNull(copyFiles.remove(dirCopyU.getName()));

		File[] filesU = dirCopyU.listFiles(noCrc);
		File[] filesQ = dirCopyQ.listFiles(noCrc);

		assertEquals(filesU.length, 3);
		assertEquals(filesU.length, filesQ.length);

		Arrays.sort(filesU);
		Arrays.sort(filesQ);

		for (int i = 0; i < filesU.length; i++) {
			assertEquals(filesU[i].getName(), filesQ[i].getName());
			copyFiles.remove(filesU[i].getName());
			if (filesU[i].isDirectory()) {
				assertEquals(filesU[i].listFiles(noCrc).length,1);
				assertEquals(filesQ[i].listFiles(noCrc).length,1);
				copyFiles.remove(filesU[i].listFiles(noCrc)[0].getName());
			}
		}

		assertTrue(copyFiles.isEmpty());

	}


}
