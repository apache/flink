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

package org.apache.flink.table.filesystem;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.impl.StreamFormatAdapter;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.reader.TextLineFormat;
import org.apache.flink.connector.file.src.util.Utils;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.FileUtils;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test for {@link LimitableBulkFormat}.
 */
public class LimitableBulkFormatTest {

	@ClassRule
	public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

	@Test
	public void test() throws IOException {
		// prepare file
		File file = TEMP_FOLDER.newFile();
		file.createNewFile();
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < 10000; i++) {
			builder.append(i).append("\n");
		}
		FileUtils.writeFileUtf8(file, builder.toString());

		// read
		BulkFormat<String, FileSourceSplit> format = LimitableBulkFormat.create(
				new StreamFormatAdapter<>(new TextLineFormat()), 22L);

		BulkFormat.Reader<String> reader = format.createReader(
				new Configuration(), new FileSourceSplit("id", new Path(file.toURI()), 0, file.length()));

		AtomicInteger i = new AtomicInteger(0);
		Utils.forEachRemaining(reader, s -> i.incrementAndGet());
		Assert.assertEquals(22, i.get());
	}
}
