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
package org.apache.flink.util;

import java.io.IOException;

/**
 * Interface for Flink customized PriorityQueue.
 * @param <T> Element type.
 */
public interface PriorityQueue<T> {
	/**
	 * Insert element into queue.
	 * @param element
	 * @throws IOException
	 */
	void insert(T element) throws IOException;

	/**
	 * Poll the next smallest element. Create new element instance each time.
	 * @return
	 * @throws IOException
	 */
	T next() throws IOException;

	/**
	 * Poll the next smallest element. Reuse the input element instance.
	 * @param reuse
	 * @return
	 * @throws IOException
	 */
	T next(T reuse) throws IOException;

	/**
	 * @return Return the priority queue size.
	 */
	int size();

	/**
	 * Close the priority queue, and release all assigned resources.
	 * @throws IOException
	 */
	void close() throws IOException;
}
