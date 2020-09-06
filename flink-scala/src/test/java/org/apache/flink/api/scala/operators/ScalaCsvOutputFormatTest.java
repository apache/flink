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

package org.apache.flink.api.scala.operators;

import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import scala.Tuple3;

import static org.junit.Assert.fail;

/**
 * Tests for {@link ScalaCsvOutputFormat}.
 */
public class ScalaCsvOutputFormatTest {

	private String path;
	private ScalaCsvOutputFormat<Tuple3<String, String, Integer>> csvOutputFormat;

	@Rule
	public final TemporaryFolder tmpFolder = new TemporaryFolder();

	@Before
	public void setUp() throws Exception {
		path = tmpFolder.newFile().getAbsolutePath();
		csvOutputFormat = new ScalaCsvOutputFormat<>(new Path(path));
		csvOutputFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);
		csvOutputFormat.setOutputDirectoryMode(FileOutputFormat.OutputDirectoryMode.PARONLY);
		csvOutputFormat.open(0, 1);
	}

	@Test
	public void testNullAllow() throws Exception {
		try {
			csvOutputFormat.setAllowNullValues(true);
			csvOutputFormat.writeRecord(new Tuple3<>("One", null, 8));
		} finally {
			csvOutputFormat.close();
		}
		java.nio.file.Path p = Paths.get(path);
		Assert.assertTrue(Files.exists(p));
		List<String> lines = Files.readAllLines(Paths.get(path), StandardCharsets.UTF_8);
		Assert.assertEquals(1, lines.size());
		Assert.assertEquals("One,,8", lines.get(0));
	}

	@Test
	public void testNullDisallowOnDefault() throws Exception {
		try {
			csvOutputFormat.setAllowNullValues(false);
			csvOutputFormat.writeRecord(new Tuple3<>("One", null, 8));
			fail("should fail with an exception");
		} catch (RuntimeException e) {
			// expected
		} finally {
			csvOutputFormat.close();
		}
	}
}
