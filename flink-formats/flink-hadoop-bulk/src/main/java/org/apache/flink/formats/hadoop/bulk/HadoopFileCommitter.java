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

package org.apache.flink.formats.hadoop.bulk;

import org.apache.flink.annotation.Internal;

import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * The committer publishes an intermediate Hadoop file to the target path after
 * it finishes writing.
 */
@Internal
public interface HadoopFileCommitter {

	/**
	 * Gets the target path to commit to.
	 *
	 * @return The target path to commit to.
	 */
	Path getTargetFilePath();

	/**
	 * Gets the path of the intermediate file to commit.
	 *
	 * @return The path of the intermediate file to commit.
	 */
	Path getInProgressFilePath();

	/**
	 * Prepares the intermediates file for committing.
	 */
	void preCommit() throws IOException;

	/**
	 * Commits the in-progress file to the target path.
	 */
	void commit() throws IOException;

	/**
	 * Re-commits the in-progress file to the target path after fail-over.
	 */
	void commitAfterRecovery() throws IOException;
}
