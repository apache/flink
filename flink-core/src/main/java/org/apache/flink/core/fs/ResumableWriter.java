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

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

/**
 * The ResumableWriter creates and recovers RecoverableFsDataOutputStream.
 */
public interface ResumableWriter {

	RecoverableFsDataOutputStream open(Path path) throws IOException;

	RecoverableFsDataOutputStream recover(ResumeRecoverable resumable) throws IOException;

	RecoverableFsDataOutputStream.Committer recoverForCommit(CommitRecoverable resumable) throws IOException;

	SimpleVersionedSerializer<CommitRecoverable> getCommitRecoverableSerializer();

	SimpleVersionedSerializer<ResumeRecoverable> getResumeRecoverableSerializer();

	boolean supportsResume();

	// ------------------------------------------------------------------------

	/**
	 * A handle to an in-progress stream with a defined and persistent amount of data.
	 * The handle can be used to recover the stream and publish the result file.
	 */
	interface CommitRecoverable {}

	/**
	 * A handle to an in-progress stream with a defined and persistent amount of data.
	 * The handle can be used to recover the stream and either publish the result file
	 * or keep appending data to the stream.
	 */
	interface ResumeRecoverable extends CommitRecoverable {}
}
