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

package org.apache.flink.connector.file.src.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.file.src.reader.BulkFormat;

import javax.annotation.Nullable;

/**
 * Utility base class for iterators that accept a recycler.
 *
 * @param <E> The type of the records returned by the iterator.
 */
@Internal
abstract class RecyclableIterator<E> implements BulkFormat.RecordIterator<E> {

	@Nullable
	private final Runnable recycler;

	/**
	 * Creates a {@code RecyclableIterator} with the given optional recycler.
	 */
	protected RecyclableIterator(@Nullable Runnable recycler) {
		this.recycler = recycler;
	}

	@Override
	public void releaseBatch() {
		if (recycler != null) {
			recycler.run();
		}
	}
}
