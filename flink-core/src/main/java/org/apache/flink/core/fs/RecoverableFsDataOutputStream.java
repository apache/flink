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

package org.apache.flink.core.fs;

import org.apache.flink.core.fs.ResumableWriter.CommitRecoverable;
import org.apache.flink.core.fs.ResumableWriter.ResumeRecoverable;

import java.io.IOException;

/**
 * An output stream to a file system that can be recovered at well defined points.
 * The stream initially writes to hidden files or temp files and only creates the
 * target file once it is closed and "committed".
 */
public abstract class RecoverableFsDataOutputStream extends FSDataOutputStream {

	/**
	 * Ensures all data so far is persistent (similar to {@link #sync()}) and returns
	 * a handle to recover the stream at the current position.
	 */
	public abstract ResumeRecoverable persist() throws IOException;

	/**
	 * Closes the stream, ensuring persistence of all data (similar to {@link #sync()}).
	 * This returns a Committer that can be used to publish (make visible) the file
	 * that the stream was writing to.
	 */
	public abstract Committer closeForCommit() throws IOException;

	@Override
	public abstract void close() throws IOException;

	// ------------------------------------------------------------------------

	/**
	 * A committer can publish the file of a stream that was closed.
	 * The Committer can be recovered via a {@link CommitRecoverable}.
	 */
	public interface Committer {

		void commit() throws IOException;

		void commitAfterRecovery() throws IOException;

		CommitRecoverable getRecoverable();
	}
}
