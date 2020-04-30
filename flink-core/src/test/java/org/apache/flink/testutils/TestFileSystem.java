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

package org.apache.flink.testutils;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileStatus;
import org.apache.flink.core.fs.local.LocalFileSystem;

import java.io.IOException;
import java.net.URI;

/**
 * A test file system. This also has a service entry in the test
 * resources, to be loaded during tests.
 */
public class TestFileSystem extends LocalFileSystem {

	public static final String SCHEME = "test";

	private static int streamOpenCounter;
	
	public static int getNumtimeStreamOpened() {
		return streamOpenCounter;
	}
	
	public static void resetStreamOpenCounter() {
		streamOpenCounter = 0;
	}
	
	@Override
	public FSDataInputStream open(Path f, int bufferSize) throws IOException {
		streamOpenCounter++;
		return super.open(f, bufferSize);
	}

	@Override
	public FSDataInputStream open(Path f) throws IOException {
		streamOpenCounter++;
		return super.open(f);
	}
	
	@Override
	public FileStatus getFileStatus(Path f) throws IOException {
		LocalFileStatus status = (LocalFileStatus) super.getFileStatus(f);
		return new LocalFileStatus(status.getFile(), this);
	}
	
	@Override
	public FileStatus[] listStatus(Path f) throws IOException {
		FileStatus[] stati = super.listStatus(f);
		LocalFileStatus[] newStati = new LocalFileStatus[stati.length];
		for (int i = 0; i < stati.length; i++) {
			newStati[i] = new LocalFileStatus(((LocalFileStatus) stati[i]).getFile(), this);
		}
		return newStati;
	}

	@Override
	public URI getUri() {
		return URI.create(SCHEME + ":///");
	}

	// ------------------------------------------------------------------------

	public static final class TestFileSystemFactory implements FileSystemFactory {

		@Override
		public String getScheme() {
			return SCHEME;
		}

		@Override
		public FileSystem create(URI fsUri) throws IOException {
			return new TestFileSystem();
		}
	}
}
