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

package org.apache.flink.connectors.hive.read;

import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.FileUtils;

import org.apache.hadoop.mapred.FileSplit;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

/**
 * Test for {@link HiveTableFileInputFormat}.
 */
public class HiveTableFileInputFormatTest {

	@ClassRule
	public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

	@Test
	public void testSplit() throws IOException {
		File file = TEMPORARY_FOLDER.newFile();
		FileUtils.writeFileUtf8(file, "hahahahahahaha");
		FileInputSplit split = new FileInputSplit(0, new Path(file.getPath()), 0, -1, null);
		FileSplit fileSplit = HiveTableFileInputFormat.toHadoopFileSplit(split);
		Assert.assertEquals(14, fileSplit.getLength());
	}
}
