/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.tests.util;

import org.apache.flink.tests.util.activation.OperatingSystemRestriction;
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Tests for {@link TestUtils}.
 */
public class TestUtilsTest extends TestLogger {

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@BeforeClass
	public static void setupClass() {
		OperatingSystemRestriction.forbid("Symbolic links usually require special permissions on Windows.", OperatingSystem.WINDOWS);
	}

	@Test
	public void copyDirectory() throws IOException {
		Path[] files = {
			Paths.get("file1"),
			Paths.get("dir1", "file2"),
		};

		Path source = temporaryFolder.newFolder("source").toPath();
		for (Path file : files) {
			Files.createDirectories(source.resolve(file).getParent());
			Files.createFile(source.resolve(file));
		}

		Path symbolicLink = source.getParent().resolve("link");
		Files.createSymbolicLink(symbolicLink, source);

		Path target = source.getParent().resolve("target");
		TestUtils.copyDirectory(symbolicLink, target);

		for (Path file: files) {
			Assert.assertTrue(Files.exists(target.resolve(file)));
		}
	}
}
