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

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * The interface for a source reader which is responsible for reading the records from
 * the source splits assigned by {@link SplitEnumerator}.
 *
 * @param <T> The type of the record emitted by this source reader.
 * @param <SplitT> The type of the the source splits.
 */
@Public
public interface SourceReader<T, SplitT extends SourceSplit> extends Serializable, AutoCloseable {

	/**
	 * Start the reader.
	 */
	void start();

	/**
	 * Poll the next available record into the {@link SourceOutput}.
	 *
	 * <p>The implementation must make sure this method is non-blocking.
	 *
	 * <p>Although the implementation can emit multiple records into the given SourceOutput,
	 * it is recommended not doing so. Instead, emit one record into the SourceOutput
	 * and return a {@link Status#AVAILABLE_NOW} to let the caller thread
	 * know there are more records available.
	 *
	 * @return The {@link Status} of the SourceReader after the method invocation.
	 */
	Status pollNext(SourceOutput<T> sourceOutput) throws Exception;

	/**
	 * Checkpoint on the state of the source.
	 *
	 * @return the state of the source.
	 */
	List<SplitT> snapshotState();

	/**
	 * @return a future that will be completed once there is a record available to poll.
	 */
	CompletableFuture<Void> isAvailable();

	/**
	 * Adds a list of splits for this reader to read.
	 *
	 * @param splits The splits assigned by the split enumerator.
	 */
	void addSplits(List<SplitT> splits);

	/**
	 * Handle a source event sent by the {@link SplitEnumerator}.
	 *
	 * @param sourceEvent the event sent by the {@link SplitEnumerator}.
	 */
	void handleSourceEvents(SourceEvent sourceEvent);

	/**
	 * The status of this reader.
	 */
	enum Status {
		/** The next record is available right now. */
		AVAILABLE_NOW,
		/** The next record will be available later. */
		AVAILABLE_LATER,
		/** The source reader has completed all the reading work. */
		FINISHED
	}
}
