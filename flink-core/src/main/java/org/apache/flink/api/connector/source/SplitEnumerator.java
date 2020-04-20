/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.apache.flink.api.connector.source;

import org.apache.flink.annotation.Public;

import java.io.IOException;
import java.util.List;

/**
 * A interface of a split enumerator responsible for the followings:
 * 1. discover the splits for the {@link SourceReader} to read.
 * 2. assign the splits to the source reader.
 */
@Public
public interface SplitEnumerator<SplitT extends SourceSplit, CheckpointT> extends AutoCloseable {

	/**
	 * Start the split enumerator.
	 *
	 * <p>The default behavior does nothing.
	 */
	void start();

	/**
	 * Handles the source event from the source reader.
	 *
	 * @param subtaskId the subtask id of the source reader who sent the source event.
	 * @param sourceEvent the source event from the source reader.
	 */
	void handleSourceEvent(int subtaskId, SourceEvent sourceEvent);

	/**
	 * Add a split back to the split enumerator. It will only happen when a {@link SourceReader} fails
	 * and there are splits assigned to it after the last successful checkpoint.
	 *
	 * @param splits The split to add back to the enumerator for reassignment.
	 * @param subtaskId The id of the subtask to which the returned splits belong.
	 */
	void addSplitsBack(List<SplitT> splits, int subtaskId);

	/**
	 * Add a new source reader with the given subtask ID.
	 *
	 * @param subtaskId the subtask ID of the new source reader.
	 */
	void addReader(int subtaskId);

	/**
	 * Checkpoints the state of this split enumerator.
	 *
	 * @return an object containing the state of the split enumerator.
	 * @throws Exception when the snapshot cannot be taken.
	 */
	CheckpointT snapshotState() throws Exception;

	/**
	 * Called to close the enumerator, in case it holds on to any resources, like threads or
	 * network connections.
	 */
	@Override
	void close() throws IOException;
}
