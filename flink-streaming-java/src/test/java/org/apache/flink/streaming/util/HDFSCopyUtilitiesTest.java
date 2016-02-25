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
package org.apache.flink.streaming.util;

import org.apache.flink.core.fs.Path;
import org.apache.flink.util.OperatingSystem;
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

import static org.junit.Assert.assertTrue;

public class HDFSCopyUtilitiesTest {

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Before
	public void checkOperatingSystem() {
		Assume.assumeTrue("This test can't run successfully on Windows.", !OperatingSystem.isWindows());
	}


	/**
	 * This test verifies that a hadoop configuration is correctly read in the external
	 * process copying tools.
	 */
	@Test
	public void testCopyFromLocal() throws Exception {

		File testFolder = tempFolder.newFolder();

		File originalFile = new File(testFolder, "original");
		File copyFile = new File(testFolder, "copy");

		try (DataOutputStream out = new DataOutputStream(new FileOutputStream(originalFile))) {
			out.writeUTF("Hello there, 42!");
		}

		HDFSCopyFromLocal.copyFromLocal(
				originalFile,
				new Path(copyFile.getAbsolutePath()).toUri());

		try (DataInputStream in = new DataInputStream(new FileInputStream(copyFile))) {
			assertTrue(in.readUTF().equals("Hello there, 42!"));

		}
	}

	/**
	 * This test verifies that a hadoop configuration is correctly read in the external
	 * process copying tools.
	 */
	@Test
	public void testCopyToLocal() throws Exception {

		File testFolder = tempFolder.newFolder();

		File originalFile = new File(testFolder, "original");
		File copyFile = new File(testFolder, "copy");

		try (DataOutputStream out = new DataOutputStream(new FileOutputStream(originalFile))) {
			out.writeUTF("Hello there, 42!");
		}

		HDFSCopyToLocal.copyToLocal(
				new Path(originalFile.getAbsolutePath()).toUri(),
				copyFile);

		try (DataInputStream in = new DataInputStream(new FileInputStream(copyFile))) {
			assertTrue(in.readUTF().equals("Hello there, 42!"));

		}
	}

}
