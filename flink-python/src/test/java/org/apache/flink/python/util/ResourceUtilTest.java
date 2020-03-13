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
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link ResourceUtil}.
 */
public class ResourceUtilTest {

	@Test
	public void testExtractUdfRunnerFromResource() throws IOException, InterruptedException {
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
			File file = ResourceUtil.extractUdfRunner(tmpdir.getAbsolutePath());
			assertEquals(file, new File(tmpdir, "pyflink-udf-runner.sh"));
			assertTrue(file.canExecute());
		} finally {
			hook.run();
			Runtime.getRuntime().removeShutdownHook(hook);
		}
	}
}
