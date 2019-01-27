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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.runtime.event.TaskEvent;

import java.io.IOException;
import java.util.Optional;

/**
 * An input gate consumes one or more partitions of a single produced intermediate result.
 *
 * <p> Each intermediate result is partitioned over its producing parallel subtasks; each of these
 * partitions is furthermore partitioned into one or more subpartitions.
 *
 * <p> As an example, consider a map-reduce program, where the map operator produces data and the
 * reduce operator consumes the produced data.
 *
 * <pre>{@code
 * +-----+              +---------------------+              +--------+
 * | Map | = produce => | Intermediate Result | <= consume = | Reduce |
 * +-----+              +---------------------+              +--------+
 * }</pre>
 *
 * <p> When deploying such a program in parallel, the intermediate result will be partitioned over its
 * producing parallel subtasks; each of these partitions is furthermore partitioned into one or more
 * subpartitions.
 *
 * <pre>{@code
 *                            Intermediate result
 *               +-----------------------------------------+
 *               |                      +----------------+ |              +-----------------------+
 * +-------+     | +-------------+  +=> | Subpartition 1 | | <=======+=== | Input Gate | Reduce 1 |
 * | Map 1 | ==> | | Partition 1 | =|   +----------------+ |         |    +-----------------------+
 * +-------+     | +-------------+  +=> | Subpartition 2 | | <==+    |
 *               |                      +----------------+ |    |    | Subpartition request
 *               |                                         |    |    |
 *               |                      +----------------+ |    |    |
 * +-------+     | +-------------+  +=> | Subpartition 1 | | <==+====+
 * | Map 2 | ==> | | Partition 2 | =|   +----------------+ |    |         +-----------------------+
 * +-------+     | +-------------+  +=> | Subpartition 2 | | <==+======== | Input Gate | Reduce 2 |
 *               |                      +----------------+ |              +-----------------------+
 *               +-----------------------------------------+
 * }</pre>
 *
 * <p> In the above example, two map subtasks produce the intermediate result in parallel, resulting
 * in two partitions (Partition 1 and 2). Each of these partitions is further partitioned into two
 * subpartitions -- one for each parallel reduce subtask. As shown in the Figure, each reduce task
 * will have an input gate attached to it. This will provide its input, which will consist of one
 * subpartition from each partition of the intermediate result.
 */
public interface InputGate {

	/**
	 * Gets number of input channels.
	 *
	 * @return the number of input channels
	 */
	int getNumberOfInputChannels();

	/**
	 * Check this input gate is finished or not.
	 *
	 * @return true if is finished, false otherwise
	 */
	boolean isFinished();

	/**
	 * Check is there any data available currently.
	 *
	 * @return true if there is some data available, false otherwise
	 */
	boolean moreAvailable();

	/**
	 * Send the request of partitions.
	 *
	 * @throws IOException          the io exception
	 * @throws InterruptedException the interrupted exception
	 */
	void requestPartitions() throws IOException, InterruptedException;

	/**
	 * Blocking call waiting for next {@link BufferOrEvent}.
	 *
	 * @return {@code Optional.empty()} if {@link #isFinished()} returns true.
	 * @throws IOException          the io exception
	 * @throws InterruptedException the interrupted exception
	 */
	Optional<BufferOrEvent> getNextBufferOrEvent() throws IOException, InterruptedException;

	/**
	 * Blocking call waiting for next {@link BufferOrEvent} on the given sub {@link InputGate} when
	 * being compounded InputGate.
	 *
	 * <p>If being single InputGate, its behavior must be equivalent to {@link #getNextBufferOrEvent()}.
	 *
	 * @param subInputGate the sub input gate
	 * @return {@code Optional.empty()} if {@link #isFinished()} returns true.
	 * @throws IOException          the io exception
	 * @throws InterruptedException the interrupted exception
	 */
	Optional<BufferOrEvent> getNextBufferOrEvent(InputGate subInputGate) throws IOException, InterruptedException;

	/**
	 * Poll the {@link BufferOrEvent}.
	 *
	 * @return {@code Optional.empty()} if there is no data to return or if {@link #isFinished()} returns true.
	 * @throws IOException          the io exception
	 * @throws InterruptedException the interrupted exception
	 */
	Optional<BufferOrEvent> pollNextBufferOrEvent() throws IOException, InterruptedException;

	/**
	 * Poll the {@link BufferOrEvent} on the given sub {@link InputGate}.
	 *
	 * @param subInputGate the given sub {@link InputGate}.
	 * @return {@code Optional.empty()} if there is no data to return or if {@link #isFinished()} returns true.
	 * @throws IOException          the io exception
	 * @throws InterruptedException the interrupted exception
	 */
	Optional<BufferOrEvent> pollNextBufferOrEvent(InputGate subInputGate) throws IOException, InterruptedException;

	/**
	 * Send task event.
	 *
	 * @param event the event
	 * @throws IOException the io exception
	 */
	void sendTaskEvent(TaskEvent event) throws IOException;

	/**
	 * Register a listener that observe not empty event.
	 * Input gate accepts multiple listeners.
	 * These listeners would be notified by the registering order.
	 *
	 * @param listener the listener
	 */
	void registerListener(InputGateListener listener);

	/**
	 * Gets page size.
	 *
	 * @return the page size
	 */
	int getPageSize();

	/**
	 * Get the number of all sub {@link InputGate}.
	 *
	 * @return 0 if this is a single InputGate, &gt 0 otherwise.
	 */
	int getSubInputGateCount();

	/**
	 * Get sub {@link InputGate} from the compounded InputGate.
	 *
	 * @param index The index of the sub InputGate.
	 * @return null if this is a single InputGate.
	 */
	InputGate getSubInputGate(int index);

	/**
	 * Get all the {@link InputChannel}s in this {@link InputGate}.
	 *
	 * @return an array contains all the input channels, the index of an input channel
	 *         in the array equals to its channel index in the input gate.
	 */
	InputChannel[] getAllInputChannels();
}
