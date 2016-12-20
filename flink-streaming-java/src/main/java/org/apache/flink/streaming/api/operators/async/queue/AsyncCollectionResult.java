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

package org.apache.flink.streaming.api.operators.async.queue;

import org.apache.flink.annotation.Internal;

import java.util.Collection;

/**
 * {@link AsyncResult} sub class for asynchronous result collections.
 *
 * @param <T> Type of the collection elements.
 */
@Internal
public interface AsyncCollectionResult<T> extends AsyncResult {

	boolean hasTimestamp();

	long getTimestamp();

	/**
	 * Return the asynchronous result collection.
	 *
	 * @return the asynchronous result collection
	 * @throws Exception if the asynchronous result collection could not be completed
	 */
	Collection<T> get() throws Exception;
}
