/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.annotation.Internal;

import java.io.IOException;

/**
 * The {@link Bucket} uses the {@link InProgressFileWriter} to write element to a part file.
 */
@Internal
public interface InProgressFileWriter<IN, BucketID> extends PartFileInfo<BucketID> {

	/**
	 * Write a element to the part file.
	 * @param element the element to be written.
	 * @param currentTime the writing time.
	 * @throws IOException Thrown if writing the element fails.
	 */
	void write(final IN element, final long currentTime) throws IOException;

	/**
	 * @return The state of the current part file.
	 * @throws IOException Thrown if persisting the part file fails.
	 */
	InProgressFileRecoverable persist() throws IOException;


	/**
	 * @return The state of the pending part file. {@link Bucket} uses this to commit the pending file.
	 * @throws IOException Thrown if an I/O error occurs.
	 */
	PendingFileRecoverable closeForCommit() throws IOException;

	/**
	 * Dispose the part file.
	 */
	void dispose();

	// ------------------------------------------------------------------------


	 /**
	 * A handle can be used to recover in-progress file..
	 */
	interface InProgressFileRecoverable extends PendingFileRecoverable {}


	/**
	 * The handle can be used to recover pending file.
	 */
	interface PendingFileRecoverable {}
}
