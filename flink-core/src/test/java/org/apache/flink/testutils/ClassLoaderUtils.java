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

package org.apache.flink.testutils;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * Utilities to create class loaders.
 */
public class ClassLoaderUtils {
	public static URLClassLoader compileAndLoadJava(File root, String filename, String source) throws
		IOException {
		File file = writeSourceFile(root, filename, source);

		compileClass(file);

		return new URLClassLoader(
			new URL[]{root.toURI().toURL()},
			Thread.currentThread().getContextClassLoader());
	}

	private static File writeSourceFile(File root, String filename, String source) throws IOException {
		File file = new File(root, filename);
		FileWriter fileWriter = new FileWriter(file);

		fileWriter.write(source);
		fileWriter.close();

		return file;
	}

	private static int compileClass(File sourceFile) {
		JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
		return compiler.run(null, null, null, "-proc:none", sourceFile.getPath());
	}
}
