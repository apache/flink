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

package org.apache.flink.table.sources;

/**
 * The configuration for lookupable table source.
 */
public class LookupConfig {

	/**
	 * Output mode for asynchronous operations.
	 */
	public enum AsyncOutputMode { ORDERED, UNORDERED}

	/** Defines whether to enable async lookup mode, default is false. */
	private boolean asyncEnabled = false;

	/** Defines output mode for asynchronous operation, ordered or unordered, default is ordered. */
	private AsyncOutputMode asyncOutputMode = AsyncOutputMode.ORDERED;

	/** Defines async timeout ms, default is 180s. */
	private long asyncTimeoutMs = 180;

	/** Defines async buffer capacity, default is 100. */
	private int asyncBufferCapacity = 100;

	public boolean isAsyncEnabled() {
		return asyncEnabled;
	}

	public void setAsyncEnabled(boolean asyncEnabled) {
		this.asyncEnabled = asyncEnabled;
	}

	public AsyncOutputMode getAsyncOutputMode() {
		return asyncOutputMode;
	}

	public void setAsyncOutputMode(AsyncOutputMode asyncOutputMode) {
		this.asyncOutputMode = asyncOutputMode;
	}

	public long getAsyncTimeoutMs() {
		return asyncTimeoutMs;
	}

	public void setAsyncTimeoutMs(long asyncTimeoutMs) {
		this.asyncTimeoutMs = asyncTimeoutMs;
	}

	public int getAsyncBufferCapacity() {
		return asyncBufferCapacity;
	}

	public void setAsyncBufferCapacity(int asyncBufferCapacity) {
		this.asyncBufferCapacity = asyncBufferCapacity;
	}
}
