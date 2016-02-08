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
package org.apache.flink.streaming.api.windowing.evictors;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import java.io.Serializable;

/**
 * An {@code Evictor} can remove elements from a pane before it is being processed and after
 * window evaluation was triggered by a
 * {@link org.apache.flink.streaming.api.windowing.triggers.Trigger}.
 *
 * <p>
 * A pane is the bucket of elements that have the same key (assigned by the
 * {@link org.apache.flink.api.java.functions.KeySelector}) and same {@link Window}. An element can
 * be in multiple panes of it was assigned to multiple windows by the
 * {@link org.apache.flink.streaming.api.windowing.assigners.WindowAssigner}. These panes all
 * have their own instance of the {@code Evictor}.
 *
 * @param <T> The type of elements that this {@code Evictor} can evict.
 * @param <W> The type of {@link Window Windows} on which this {@code Evictor} can operate.
 */
@PublicEvolving
public interface Evictor<T, W extends Window> extends Serializable {

	/**
	 * Computes how many elements should be removed from the pane. The result specifies how
	 * many elements should be removed from the beginning.
	 *
	 * @param elements The elements currently in the pane.
	 * @param size The current number of elements in the pane.
	 * @param window The {@link Window}
	 */
	int evict(Iterable<StreamRecord<T>> elements, int size, W window);
}

