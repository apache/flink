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

package org.apache.flink.core.fs.local;

import org.apache.flink.core.fs.AbstractRecoverableWriterTest;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for the {@link LocalRecoverableWriter}.
 */
public class LocalFileSystemRecoverableWriterTest extends AbstractRecoverableWriterTest {

	@ClassRule
	public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

	@Override
	public Path getBasePath() throws Exception {
		return new Path(TEMP_FOLDER.newFolder().toURI());
	}

	@Override
	public FileSystem initializeFileSystem() {
		return FileSystem.getLocalFileSystem();
	}
}
