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

package org.apache.flink.fs.gcs.common.writer;

import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class GcsCommitter implements RecoverableFsDataOutputStream.Committer {
	private static final Logger LOG = LoggerFactory.getLogger(GcsCommitter.class);

	private final GcsRecoverable recoverable;

	public GcsCommitter(GcsRecoverable recoverable) {
		this.recoverable = recoverable;
	}

	/**
	 * Commits the file, making it visible. The file will contain the exact data
	 * as when the committer was created.
	 */
	@Override
	public void commit() {
		LOG.info("Commit: {}", recoverable.toString());
	}

	/**
	 * Commits the file, making it visible. The file will contain the exact data
	 * as when the committer was created.
	 *
	 * <p>This method tolerates situations where the file was already committed and
	 * will not raise an exception in that case. This is important for idempotent
	 * commit retries as they need to happen after recovery.
	 *
	 * @throws IOException Thrown if committing fails.
	 */
	@Override
	public void commitAfterRecovery() throws IOException {
		LOG.info("CommitAfterRecovery {}", recoverable.toString());
	}

	/**
	 * Gets a recoverable object to recover the committer. The recovered committer
	 * will commit the file with the exact same data as this committer would commit
	 * it.
	 */
	@Override
	public RecoverableWriter.CommitRecoverable getRecoverable() {
		LOG.info("GetRecoverable {}", recoverable.toString());
		return this.recoverable;
	}
}
