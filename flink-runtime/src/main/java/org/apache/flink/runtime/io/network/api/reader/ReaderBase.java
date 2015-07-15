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

package org.apache.flink.runtime.io.network.api.reader;

import java.io.IOException;

import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.util.event.EventListener;

/**
 * The basic API for every reader.
 */
public interface ReaderBase {

	/**
	 * Returns whether the reader has consumed the input.
	 */
	boolean isFinished();

	// ------------------------------------------------------------------------
	// Task events
	// ------------------------------------------------------------------------

	void sendTaskEvent(TaskEvent event) throws IOException;

	void registerTaskEventListener(EventListener<TaskEvent> listener, Class<? extends TaskEvent> eventType);

	// ------------------------------------------------------------------------
	// Iterations
	// ------------------------------------------------------------------------

	void setIterativeReader();

	void startNextSuperstep();

	boolean hasReachedEndOfSuperstep();

	/**
	 * Setter for the reporter, e.g. for the number of records emitted and the number of bytes read.
	 */
	void setReporter(AccumulatorRegistry.Reporter reporter);

}
