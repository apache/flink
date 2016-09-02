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

package org.apache.flink.runtime.concurrent;

/**
 * Flink's completable future abstraction. A completable future can be completed with a regular
 * value or an exception.
 *
 * @param <T> type of the future's value
 */
public interface CompletableFuture<T> extends Future<T> {

	/**
	 * Completes the future with the given value. The complete operation only succeeds if the future
	 * has not been completed before. Whether it is successful or not is returned by the method.
	 *
	 * @param value to complete the future with
	 * @return true if the completion was successful; otherwise false
	 */
	boolean complete(T value);

	/**
	 * Completes the future with the given exception. The complete operation only succeeds if the
	 * future has not been completed before. Whether it is successful or not is returned by the
	 * method.
	 *
	 * @param t the exception to complete the future with
	 * @return true if the completion was successful; otherwise false
	 */
	boolean completeExceptionally(Throwable t);
}
