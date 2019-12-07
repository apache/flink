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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.flink.python.util.ResourceUtil.extractBuiltInDependencies;

/**
 * The program that extracts the internal python libraries and join their absolute paths to append to PYTHONPATH. it can
 * accept one argument as the temp directory.
 */
public class PythonResourceExtractor {

	public static void main(String[] args) throws IOException, InterruptedException {
		String tmpdir = System.getProperty("java.io.tmpdir");

		List<File> files = extractBuiltInDependencies(
			tmpdir,
			UUID.randomUUID().toString(),
			true);

		System.out.print(
			files.stream()
				.map(File::getAbsolutePath)
				.collect(Collectors.joining(File.pathSeparator)));
	}
}
