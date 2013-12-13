/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.pact.common.testutils;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.Map;

import eu.stratosphere.nephele.fs.FSDataInputStream;
import eu.stratosphere.nephele.fs.FileStatus;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.fs.file.LocalFileStatus;
import eu.stratosphere.nephele.fs.file.LocalFileSystem;

public class TestFileSystem extends LocalFileSystem {
	
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
	
	public static void registerTestFileSysten() throws Exception {
		Class<FileSystem> fsClass = FileSystem.class;
		Field dirField = fsClass.getDeclaredField("FSDIRECTORY");
		
		dirField.setAccessible(true);
		@SuppressWarnings("unchecked")
		Map<String, String> map = (Map<String, String>) dirField.get(null);
		dirField.setAccessible(false);
		
		map.put("test", TestFileSystem.class.getName());
	}

	@Override
	public URI getUri() {
		return URI.create("test:///");
	}
}
