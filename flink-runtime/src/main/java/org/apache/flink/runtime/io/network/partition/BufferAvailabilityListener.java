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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.io.network.buffer.BufferConsumer;

import java.io.IOException;

/**
 * Listener interface implemented by consumers of {@link ResultSubpartitionView}
 * that want to be notified of availability of further buffers.
 */
public interface BufferAvailabilityListener {

	/**
	 * Called whenever there might be new data available.
	 */
	void notifyDataAvailable();

	/**
	 * Allows the listener to react to a priority event before it is added to the outgoing buffer queue.
	 *
	 * @return true if the event has been fully processed and should not be added to the buffer queue.
	 */
	default boolean notifyPriorityEvent(BufferConsumer eventBufferConsumer) throws IOException {
		return false;
	}
}
