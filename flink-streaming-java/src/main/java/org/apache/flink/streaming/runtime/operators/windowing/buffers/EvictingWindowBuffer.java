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

/**
 * A {@code WindowBuffer} that can also evict elements from the buffer. The order in which
 * the elements are added is preserved. Elements can only be evicted started from the beginning of
 * the buffer.
 *
 * @param <T> The type of elements that this {@code WindowBuffer} can store.
 */

public interface EvictingWindowBuffer<T> extends WindowBuffer<T> {

	/**
	 * Removes the given number of elements, starting from the beginning.
	 * @param count The number of elements to remove.
	 */
	void removeElements(int count);
}
