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
import org.apache.flink.metrics.MetricGroup;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;

/**
 * A context class for the {@link SplitEnumerator}. This class serves the following purposes:
 * 1. Host information necessary for the SplitEnumerator to make split assignment decisions.
 * 2. Accept and track the split assignment from the enumerator.
 * 3. Provide a managed threading model so the split enumerators do not need to create their
 *    own internal threads.
 *
 * @param <SplitT> the type of the splits.
 */
@Public
public interface SplitEnumeratorContext<SplitT extends SourceSplit> {

	MetricGroup metricGroup();

	/**
	 * Send a source event to a source reader. The source reader is identified by its subtask id.
	 *
	 * @param subtaskId the subtask id of the source reader to send this event to.
	 * @param event the source event to send.
	 */
	void sendEventToSourceReader(int subtaskId, SourceEvent event);

	/**
	 * Get the number of subtasks.
	 *
	 * @return the number of subtasks.
	 */
	int numSubtasks();

	/**
	 * Get the currently registered readers. The mapping is from subtask id to the reader info.
	 *
	 * @return the currently registered readers.
	 */
	Map<Integer, ReaderInfo> registeredReaders();

	/**
	 * Assign the splits.
	 *
	 * @param newSplitAssignments the new split assignments to add.
	 */
	void assignSplits(SplitsAssignment<SplitT> newSplitAssignments);

	/**
	 * Invoke the callable and handover the return value to the handler which will be executed
	 * by the source coordinator.
	 *
	 * <p>It is important to make sure that the callable should not modify
	 * any shared state. Otherwise the there might be unexpected behavior.
	 *
	 * @param callable a callable to call.
	 * @param handler a handler that handles the return value of or the exception thrown from the callable.
	 */
	<T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler);

	/**
	 * Invoke the callable and handover the return value to the handler which will be executed
	 * by the source coordinator.
	 *
	 * <p>It is important to make sure that the callable should not modify
	 * any shared state. Otherwise the there might be unexpected behavior.
	 *
	 * @param callable the callable to call.
	 * @param handler a handler that handles the return value of or the exception thrown from the callable.
	 * @param initialDelay the initial delay of calling the callable.
	 * @param period the period between two invocations of the callable.
	 */
	<T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler, long initialDelay, long period);
}
