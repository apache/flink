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

package org.apache.flink.tests.util.util;

import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Tests for {@link FileUtils}.
 */
public class FileUtilsTest extends TestLogger {

	@ClassRule
	public static final TemporaryFolder TMP = new TemporaryFolder();

	private static final List<String> ORIGINAL_LINES = Collections.unmodifiableList(Arrays.asList("line1", "line2", "line3"));
	private Path testFile;

	@Before
	public void setupFile() throws IOException {
		Path path = TMP.newFile().toPath();

		Files.write(path, ORIGINAL_LINES);

		testFile = path;
	}

	@Test
	public void replaceSingleMatch() throws IOException {
		FileUtils.replace(testFile, Pattern.compile("line1"), matcher -> "removed");

		Assert.assertEquals(Arrays.asList("removed", ORIGINAL_LINES.get(1), ORIGINAL_LINES.get(2)), Files.readAllLines(testFile));
	}

	@Test
	public void replaceMultipleMatch() throws IOException {
		FileUtils.replace(testFile, Pattern.compile("line(.*)"), matcher -> matcher.group(1));

		Assert.assertEquals(Arrays.asList("1", "2", "3"), Files.readAllLines(testFile));
	}

	@Test
	public void replaceWithEmptyLine() throws IOException {
		FileUtils.replace(testFile, Pattern.compile("line2"), matcher -> "");

		Assert.assertEquals(Arrays.asList(ORIGINAL_LINES.get(0), "", ORIGINAL_LINES.get(2)), Files.readAllLines(testFile));
	}
}
