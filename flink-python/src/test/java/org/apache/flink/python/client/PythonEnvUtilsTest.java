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

package org.apache.flink.python.client;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Tests for the {@link PythonEnvUtils}.
 */
public class PythonEnvUtilsTest {
	private Path sourceTmpDirPath;
	private Path targetTmpDirPath;
	private FileSystem sourceFs;
	private FileSystem targetFs;

	@Before
	public void prepareTestEnvironment() {
		String sourceTmpDir = System.getProperty("java.io.tmpdir") +
			File.separator + "source_" + UUID.randomUUID();
		String targetTmpDir = System.getProperty("java.io.tmpdir") +
			File.separator + "target_" + UUID.randomUUID();

		sourceTmpDirPath = new Path(sourceTmpDir);
		targetTmpDirPath = new Path(targetTmpDir);
		try {
			sourceFs = sourceTmpDirPath.getFileSystem();
			if (sourceFs.exists(sourceTmpDirPath)) {
				sourceFs.delete(sourceTmpDirPath, true);
			}
			sourceFs.mkdirs(sourceTmpDirPath);
			targetFs = targetTmpDirPath.getFileSystem();
			if (targetFs.exists(targetTmpDirPath)) {
				targetFs.delete(targetTmpDirPath, true);
			}
			targetFs.mkdirs(targetTmpDirPath);
		} catch (IOException e) {
			throw new RuntimeException("initial PythonUtil test environment failed");
		}
	}

	@Test
	public void testStartPythonProcess() {
		PythonEnvUtils.PythonEnvironment pythonEnv = new PythonEnvUtils.PythonEnvironment();
		pythonEnv.workingDirectory = targetTmpDirPath.toString();
		pythonEnv.pythonPath = targetTmpDirPath.toString();
		List<String> commands = new ArrayList<>();
		Path pyPath = new Path(targetTmpDirPath, "word_count.py");
		try {
			targetFs.create(pyPath, FileSystem.WriteMode.OVERWRITE);
			File pyFile = new File(pyPath.toString());
			String pyProgram = "#!/usr/bin/python\n" +
				"# -*- coding: UTF-8 -*-\n" +
				"import sys\n" +
				"\n" +
				"if __name__=='__main__':\n" +
				"\tfilename = sys.argv[1]\n" +
				"\tfo = open(filename, \"w\")\n" +
				"\tfo.write( \"hello world\")\n" +
				"\tfo.close()";
			Files.write(pyFile.toPath(), pyProgram.getBytes(), StandardOpenOption.WRITE);
			Path result = new Path(targetTmpDirPath, "word_count_result.txt");
			commands.add(pyFile.getName());
			commands.add(result.getName());
			Process pythonProcess = PythonEnvUtils.startPythonProcess(pythonEnv, commands);
			int exitCode = pythonProcess.waitFor();
			if (exitCode != 0) {
				throw new RuntimeException("Python process exits with code: " + exitCode);
			}
			String cmdResult = new String(Files.readAllBytes(new File(result.toString()).toPath()));
			Assert.assertEquals(cmdResult, "hello world");
			pythonProcess.destroyForcibly();
			targetFs.delete(pyPath, true);
			targetFs.delete(result, true);
		} catch (IOException | InterruptedException e) {
			throw new RuntimeException("test start Python process failed " + e.getMessage());
		}
	}

	@After
	public void cleanEnvironment() {
		try {
			sourceFs.delete(sourceTmpDirPath, true);
			targetFs.delete(targetTmpDirPath, true);
		} catch (IOException e) {
			throw new RuntimeException("delete tmp dir failed " + e.getMessage());
		}
	}
}
