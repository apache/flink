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

package org.apache.flink.runtime.io.network.partition.consumer;

import java.util.concurrent.CompletableFuture;

/**
 * A general tool which is used for tracing the data availability based on future state.
 */
public class FutureBasedDataAvailability {

	public static final CompletableFuture<?> AVAILABLE = CompletableFuture.completedFuture(null);

	protected CompletableFuture<?> isAvailable = new CompletableFuture<>();

	/**
	 * @return a future that is completed if there are more data available. If there more data
	 * available immediately, {@link #AVAILABLE} should be returned.
	 */
	public CompletableFuture<?> isAvailable() {
		return isAvailable;
	}

	protected void resetIsAvailable() {
		// try to avoid volatile access in isDone()}
		if (isAvailable == AVAILABLE || isAvailable.isDone()) {
			isAvailable = new CompletableFuture<>();
		}
	}
}
