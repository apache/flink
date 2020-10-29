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

package org.apache.flink.table.filesystem.stream.compact;

import org.apache.flink.core.fs.Path;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Test base for compact operators.
 */
public abstract class AbstractCompactTestBase {

	@ClassRule
	public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

	public static final int TARGET_SIZE = 9;

	Path folder;

	@Before
	public void before() throws IOException {
		folder = new Path(TEMP_FOLDER.newFolder().getPath());
	}

	Path newFile(String name, int len) throws IOException {
		Path path = new Path(folder, name);
		File file = new File(path.getPath());
		file.delete();
		file.createNewFile();

		try (FileOutputStream out = new FileOutputStream(file)) {
			for (int i = 0; i < len; i++) {
				out.write(i);
			}
		}
		return path;
	}
}
