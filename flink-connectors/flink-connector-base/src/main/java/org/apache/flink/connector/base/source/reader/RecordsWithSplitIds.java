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

package org.apache.flink.connector.base.source.reader;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * An interface for the elements passed from the fetchers to the source reader.
 */
public interface RecordsWithSplitIds<E> {

	/**
	 * Get all the split ids.
	 *
	 * @return a collection of split ids.
	 */
	Collection<String> splitIds();

	/**
	 * Get all the records by Splits.
	 *
	 * @return a mapping from split ids to the records.
	 */
	Map<String, Collection<E>> recordsBySplits();

	/**
	 * Get the finished splits.
	 *
	 * @return the finished splits after this RecordsWithSplitIds is returned.
	 */
	Set<String> finishedSplits();
}
