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

package org.apache.flink.python.util;

import org.apache.flink.util.FileUtils;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link ResourceUtil}.
 */
public class ResourceUtilTest {

	@Test
	public void testExtractBasicDependenciesFromResource() throws IOException, InterruptedException {
		File tmpdir = File.createTempFile(UUID.randomUUID().toString(), null);
		tmpdir.delete();
		tmpdir.mkdirs();
		Thread hook = new Thread(() -> {
			try {
				FileUtils.deleteDirectory(tmpdir);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		});
		Runtime.getRuntime().addShutdownHook(hook);
		try {
			String prefix = "tmp_";
			List<File> files = ResourceUtil.extractBuiltInDependencies(
				tmpdir.getAbsolutePath(),
				prefix,
				true);
			files.forEach(file -> assertTrue(file.exists()));
			assertArrayEquals(new File[] {
				new File(tmpdir, "tmp_pyflink.zip"),
				new File(tmpdir, "tmp_py4j-0.10.8.1-src.zip"),
				new File(tmpdir, "tmp_cloudpickle-1.2.2-src.zip")}, files.toArray());
			files.forEach(File::delete);
			files = ResourceUtil.extractBuiltInDependencies(
				tmpdir.getAbsolutePath(),
				prefix,
				false);
			files.forEach(file -> assertTrue(file.exists()));
			assertArrayEquals(new File[] {
				new File(tmpdir, "tmp_pyflink.zip"),
				new File(tmpdir, "tmp_py4j-0.10.8.1-src.zip"),
				new File(tmpdir, "tmp_cloudpickle-1.2.2-src.zip"),
				new File(tmpdir, "tmp_pyflink-udf-runner.sh")}, files.toArray());
			assertTrue(new File(tmpdir, "tmp_pyflink-udf-runner.sh").canExecute());
		} finally {
			hook.run();
			Runtime.getRuntime().removeShutdownHook(hook);
		}
	}
}
