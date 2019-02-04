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

package org.apache.flink.streaming.api.functions.sink.filesystem.utils;

import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;

import java.io.IOException;

/**
 * A default implementation of the {@link RecoverableFsDataOutputStream} that does nothing.
 *
 * <p>This is to avoid to have to implement all methods for every implementation
 * used in tests.
 */
public class NoOpRecoverableFsDataOutputStream extends RecoverableFsDataOutputStream {
	@Override
	public RecoverableWriter.ResumeRecoverable persist() throws IOException {
		return null;
	}

	@Override
	public Committer closeForCommit() throws IOException {
		return null;
	}

	@Override
	public void close() throws IOException {

	}

	@Override
	public long getPos() throws IOException {
		return 0;
	}

	@Override
	public void flush() throws IOException {

	}

	@Override
	public void sync() throws IOException {

	}

	@Override
	public void write(int b) throws IOException {

	}
}
