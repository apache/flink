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

package org.apache.flink.streaming.python.api;

import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.streaming.python.api.environment.PythonEnvironmentConfig;
import org.apache.flink.streaming.python.api.functions.UtilityFunctions;

import org.apache.commons.io.FilenameUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Random;

/**
 * Allows the execution of Flink stream plan that is written in Python.
 */
public class PythonStreamBinder {
	private static final Random r = new Random(System.currentTimeMillis());
	public static final String FLINK_PYTHON_FILE_PATH = System.getProperty("java.io.tmpdir") + File.separator + "flink_streaming_plan_";

	private PythonStreamBinder() {
	}

	/**
	 * Entry point for the execution of a python streaming task.
	 *
	 * @param args pathToScript [pathToPackage1 [pathToPackageX[ - [parameter1[ parameterX]]]]
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		PythonStreamBinder binder = new PythonStreamBinder();
		binder.runPlan(args);
	}

	private void runPlan(String[] args) throws Exception {
		File script;
		if (args.length < 1) {
			System.out.println("Usage: prog <pathToScript> [<pathToPackage1> .. [<pathToPackageX]] - [parameter1]..[parameterX]");
			return;
		}
		else {
			script = new File(args[0]);
			if ((!script.exists()) || (!script.isFile())) {
				throw new FileNotFoundException("Could not find file: " + args[0]);
			}
		}

		int split = 0;
		for (int x = 0; x < args.length; x++) {
			if (args[x].compareTo("-") == 0) {
				split = x;
			}
		}

		GlobalConfiguration.loadConfiguration();

		String tmpPath = FLINK_PYTHON_FILE_PATH + r.nextLong();
		prepareFiles(tmpPath, Arrays.copyOfRange(args, 0, split == 0 ? args.length : split));

		if (split != 0) {
			String[] a = new String[args.length - split];
			a[0] = args[0];
			System.arraycopy(args, split + 1, a, 1, args.length - (split + 1));
			args = a;
		} else if (args.length > 1) {
			args = new String[]{args[0]};
		}

		UtilityFunctions.initAndExecPythonScript(new File(
			tmpPath + File.separator + PythonEnvironmentConfig.FLINK_PYTHON_PLAN_NAME), args);
	}

	/**
	 * Prepares a single package from the given python script. This involves in generating a unique main
	 * python script, which loads the user's provided script for execution. The reason for this is to create
	 * a different namespace for different script contents. In addition, it copies all relevant files
	 * to a temporary folder for execution and distribution.
	 *
	 * @param tempFilePath The path to copy all the files to
	 * @param filePaths The user's script and dependent packages/files
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	private void prepareFiles(String tempFilePath, String... filePaths) throws IOException, URISyntaxException,
		NoSuchAlgorithmException {
		System.setProperty(PythonEnvironmentConfig.PYTHON_TMP_CACHE_PATH_PROP, tempFilePath);

		Files.createDirectories(Paths.get(tempFilePath));

		String dstMainScriptFullPath = tempFilePath + File.separator + FilenameUtils.getBaseName(filePaths[0]) +
			calcPythonMd5(filePaths[0]) + ".py";

		generateAndCopyPlanFile(tempFilePath, dstMainScriptFullPath);

		//main script
		copyFile(filePaths[0], tempFilePath, FilenameUtils.getName(dstMainScriptFullPath));

		String parentDir = new File(filePaths[0]).getParent();

		//additional files/folders(modules)
		for (int x = 1; x < filePaths.length; x++) {
			boolean isRelativePath = !(new File(filePaths[x])).isAbsolute();
			if (isRelativePath) {
				filePaths[x] = parentDir + File.separator + filePaths[x];
			}
			copyFile(filePaths[x], tempFilePath, null);
		}
	}

	private void generateAndCopyPlanFile(String dstPath, String mainScriptFullPath) throws IOException {
		String moduleName = FilenameUtils.getBaseName(mainScriptFullPath);

		FileWriter fileWriter = new FileWriter(dstPath + File.separator + PythonEnvironmentConfig.FLINK_PYTHON_PLAN_NAME);
		PrintWriter printWriter = new PrintWriter(fileWriter);
		printWriter.printf("import %s\n\n", moduleName);
		printWriter.printf("%s.main()\n", moduleName);
		printWriter.close();
	}

	private void copyFile(String path, String target, String name) throws IOException, URISyntaxException {
		if (path.endsWith(File.separator)) {
			path = path.substring(0, path.length() - 1);
		}
		String identifier = name == null ? path.substring(path.lastIndexOf(File.separator)) : name;
		String tmpFilePath = target + File.separator + identifier;
		Path p = new Path(path);
		FileCache.copy(p.makeQualified(FileSystem.get(p.toUri())), new Path(tmpFilePath), true);
	}


	/**
	 * Naive MD5 calculation from the python content of the given script. Spaces, blank lines and comments
	 * are ignored.
	 *
	 * @param filePath  the full path of the given python script
	 * @return  the md5 value as a string
	 * @throws NoSuchAlgorithmException
	 * @throws IOException
	 */
	private String calcPythonMd5(String filePath) throws NoSuchAlgorithmException, IOException {
		FileReader fileReader = new FileReader(filePath);
		BufferedReader bufferedReader = new BufferedReader(fileReader);

		String line;
		MessageDigest md = MessageDigest.getInstance("MD5");
		while ((line = bufferedReader.readLine()) != null) {
			line = line.trim();
			if (line.isEmpty() || line.startsWith("#")) {
				continue;
			}
			byte[] bytes = line.getBytes();
			md.update(bytes, 0, bytes.length);
		}
		fileReader.close();

		byte[] mdBytes = md.digest();

		//convert the byte to hex format method 1
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < mdBytes.length; i++) {
			sb.append(Integer.toString((mdBytes[i] & 0xff) + 0x100, 16).substring(1));
		}
		return sb.toString();
	}
}
