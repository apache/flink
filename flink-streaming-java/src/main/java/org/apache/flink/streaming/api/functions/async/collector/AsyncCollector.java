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

package org.apache.flink.streaming.api.functions.async.collector;

import org.apache.flink.annotation.Internal;

import java.util.List;

/**
 * {@link AsyncCollector} collects data / error in user codes while processing async i/o.
 *
 * @param <OUT> Output type
 */
@Internal
public interface AsyncCollector<OUT> {
	/**
	 * Set result.
	 * <p>
	 * Note that it should be called for exactly one time in the user code.
	 * Calling this function for multiple times will cause data lose.
	 * <p>
	 * Put all results in a {@link List} and then issue {@link AsyncCollector#collect(List)}.
	 * <p>
	 * If the result is NULL, it will cause task fail. If collecting empty result set is allowable and
	 * should not cause task fail-over, then try to collect an empty list collection.
	 *
	 * @param result A list of results.
	 */
	void collect(List<OUT> result);

	/**
	 * Set error
	 *
	 * @param error A Throwable object.
	 */
	void collect(Throwable error);
}
