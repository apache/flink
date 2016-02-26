/**
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
package org.apache.flink.streaming.runtime.operators.windowing.buffers;


import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.IOException;

/**
 * A {@code WindowBuffer} is used by
 * {@link org.apache.flink.streaming.runtime.operators.windowing.WindowOperator} to store
 * the elements of one pane.
 *
 * <p>
 * A pane is the bucket of elements that have the same key (assigned by the
 * {@link org.apache.flink.api.java.functions.KeySelector}) and same {@link Window}. An element can
 * be in multiple panes of it was assigned to multiple windows by the
 * {@link org.apache.flink.streaming.api.windowing.assigners.WindowAssigner}. These panes all
 * have their own instance of the {@code Evictor}.
 *
 * @param <T> The type of elements that this {@code WindowBuffer} can store.
 * @param <O> The type of elements that this window buffer will return when asked for its contents.
 */
@Internal
public interface WindowBuffer<T, O> {

	/**
	 * Adds the element to the buffer.
	 *
	 * @param element The element to add.
	 */
	void storeElement(StreamRecord<T> element) throws Exception;

	/**
	 * Returns all elements that are currently in the buffer.
	 */
	Iterable<StreamRecord<O>> getElements();

	/**
	 * Returns all elements that are currently in the buffer. This will unwrap the contained
	 * elements from their {@link StreamRecord}.
	 */
	Iterable<O> getUnpackedElements();

	/**
	 * Returns the number of elements that are currently in the buffer.
	 */
	int size();

	/**
	 * Writes the contents of the window buffer to a {@link DataOutputView} for checkpointing.
	 */
	void snapshot(DataOutputView out) throws IOException;
}
