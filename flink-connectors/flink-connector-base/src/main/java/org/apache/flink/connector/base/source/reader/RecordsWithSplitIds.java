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

package org.apache.flink.connector.base.source.reader;

import javax.annotation.Nullable;

import java.util.Set;

/**
 * An interface for the elements passed from the fetchers to the source reader.
 */
public interface RecordsWithSplitIds<E> {

	/**
	 * Moves to the next split. This method is also called initially to move to the
	 * first split. Returns null, if no splits are left.
	 */
	@Nullable
	String nextSplit();

	/**
	 * Gets the next record from the current split. Returns null if no more records are left
	 * in this split.
	 */
	@Nullable
	E nextRecordFromSplit();

	/**
	 * Get the finished splits.
	 *
	 * @return the finished splits after this RecordsWithSplitIds is returned.
	 */
	Set<String> finishedSplits();

	/**
	 * This method is called when all records from this batch have been emitted.
	 *
	 * <p>Overriding this method gives implementations the opportunity to recycle/reuse this object,
	 * which is a performance optimization that is important for cases where the record objects are
	 * large or otherwise heavy to allocate.
	 */
	default void recycle() {}
}
