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

package org.apache.flink.client.python;

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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Tests for the {@link PythonDriverEnvUtils}.
 */
public class PythonDriverEnvUtilsTest {
	private static final String UUID_PATTERN = "[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}";

	private Path tmpDirPath;
	private FileSystem tmpDirFs;

	@Before
	public void prepareTestEnvironment() {
		String tmpDir = System.getProperty("java.io.tmpdir") +
			File.separator + "pyflink" + File.separator + UUID.randomUUID();

		tmpDirPath = new Path(tmpDir);
		try {
			tmpDirFs = tmpDirPath.getFileSystem();
			if (tmpDirFs.exists(tmpDirPath)) {
				tmpDirFs.delete(tmpDirPath, true);
			}
			tmpDirFs.mkdirs(tmpDirPath);
		} catch (IOException e) {
			throw new RuntimeException("initial PythonUtil test environment failed");
		}
	}

	@Test
	public void testPreparePythonEnvironment() throws IOException, InterruptedException {
		// xxx/a.zip, xxx/subdir/b.py, xxx/subdir/c.zip
		File a = new File(tmpDirPath.toString() + File.separator + "a.zip");
		a.createNewFile();
		File subdir = new File(tmpDirPath.toString() + File.separator + "subdir");
		subdir.mkdir();
		File b = new File(tmpDirPath.toString() + File.separator + "subdir" + File.separator + "b.py");
		b.createNewFile();
		File c = new File(tmpDirPath.toString() + File.separator + "subdir" + File.separator + "c.zip");
		c.createNewFile();

		List<Path> pyFilesList = new ArrayList<>();
		pyFilesList.add(tmpDirPath);

		PythonDriverEnvUtils.PythonEnvironment env = PythonDriverEnvUtils.preparePythonEnvironment(pyFilesList);
		Set<String> expectedPythonPaths = new HashSet<>();
		expectedPythonPaths.add(env.workingDirectory);

		String targetDir = env.workingDirectory + File.separator + tmpDirPath.getName();
		expectedPythonPaths.add(targetDir + File.separator + a.getName());
		expectedPythonPaths.add(targetDir + File.separator + "subdir" + File.separator + c.getName());

		// the parent dir for files suffixed with .py should also be added to PYTHONPATH
		expectedPythonPaths.add(targetDir + File.separator + "subdir");

		Set<String> expectedInteralLibPatterns = new HashSet<>();
		expectedInteralLibPatterns.add(
			Pattern.quote(env.workingDirectory + File.separator) + UUID_PATTERN + "pyflink\\.zip");
		expectedInteralLibPatterns.add(
			Pattern.quote(env.workingDirectory + File.separator) + UUID_PATTERN + "py4j-0\\.10\\.8\\.1-src\\.zip");
		expectedInteralLibPatterns.add(
			Pattern.quote(env.workingDirectory + File.separator) + UUID_PATTERN + "cloudpickle-1\\.2\\.2-src\\.zip");
		List<String> actualPaths = Arrays.asList(env.pythonPath.split(File.pathSeparator));
		expectedPythonPaths.addAll(
			getMatchedPaths(
				expectedInteralLibPatterns,
				actualPaths));
		Assert.assertEquals(expectedPythonPaths, new HashSet<>(actualPaths));
	}

	@Test
	public void testStartPythonProcess() {
		PythonDriverEnvUtils.PythonEnvironment pythonEnv = new PythonDriverEnvUtils.PythonEnvironment();
		pythonEnv.workingDirectory = tmpDirPath.toString();
		pythonEnv.pythonPath = tmpDirPath.toString();
		List<String> commands = new ArrayList<>();
		Path pyPath = new Path(tmpDirPath, "word_count.py");
		try {
			tmpDirFs.create(pyPath, FileSystem.WriteMode.OVERWRITE);
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
			Path result = new Path(tmpDirPath, "word_count_result.txt");
			commands.add(pyFile.getName());
			commands.add(result.getName());
			Process pythonProcess = PythonDriverEnvUtils.startPythonProcess(pythonEnv, commands);
			int exitCode = pythonProcess.waitFor();
			if (exitCode != 0) {
				throw new RuntimeException("Python process exits with code: " + exitCode);
			}
			String cmdResult = new String(Files.readAllBytes(new File(result.toString()).toPath()));
			Assert.assertEquals(cmdResult, "hello world");
			pythonProcess.destroyForcibly();
			tmpDirFs.delete(pyPath, true);
			tmpDirFs.delete(result, true);
		} catch (IOException | InterruptedException e) {
			throw new RuntimeException("test start Python process failed " + e.getMessage());
		}
	}

	@After
	public void cleanEnvironment() {
		try {
			tmpDirFs.delete(tmpDirPath, true);
		} catch (IOException e) {
			throw new RuntimeException("delete tmp dir failed " + e.getMessage());
		}
	}

	private static Set<String> getMatchedPaths(Collection<String> patterns, Collection<String> paths) {
		List<Pattern> regexPatterns = patterns.stream().map(Pattern::compile).collect(Collectors.toList());
		return paths.stream()
			.filter(
				path -> regexPatterns.stream().anyMatch(p -> p.matcher(path).matches()))
			.collect(Collectors.toSet());
	}
}
