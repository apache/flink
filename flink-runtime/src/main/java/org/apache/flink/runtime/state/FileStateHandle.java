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

package org.apache.flink.runtime.state;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.StringUtils;

import scala.util.Random;

/**
 * Statehandle that writes the checkpointed state to a random file in the
 * provided checkpoint directory. Any Flink supported File system can be used
 * but it is advised to use a filesystem that is persistent in case of node
 * failures, such as HDFS or Tachyon.
 * 
 */
public class FileStateHandle extends ByteStreamStateHandle {

	private static final long serialVersionUID = 1L;

	private String pathString;

	public FileStateHandle(Serializable state, String folder) throws IOException {
		super(state);
		this.pathString = folder + "/" + randomString();
	}

	protected OutputStream getOutputStream() throws IOException, URISyntaxException {
		return FileSystem.get(new URI(pathString)).create(new Path(pathString), true);
	}

	protected InputStream getInputStream() throws IOException, URISyntaxException {
		return FileSystem.get(new URI(pathString)).open(new Path(pathString));
	}

	private String randomString() {
		final byte[] bytes = new byte[20];
		new Random().nextBytes(bytes);
		return StringUtils.byteToHexString(bytes);
	}

	@Override
	public void discardState() throws Exception {
		FileSystem.get(new URI(pathString)).delete(new Path(pathString), false);
	}

}
