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
 * The {@link LookupConfig} is used to configure some behavior when lookup a table.
 *
 * @see LookupableTableSource#getLookupConfig()
 */
public class LookupConfig {

	public static final LookupConfig DEFAULT = LookupConfig.builder().build();

	private static final boolean DEFAULT_ASYNC_ENABLED = false;
	private static final long DEFAULT_ASYNC_TIMEOUT_MS = 180_000;
	private static final int DEFAULT_ASYNC_BUFFER_CAPACITY = 100;

	private final boolean asyncEnabled;
	private final long asyncTimeoutMs;
	private final int asyncBufferCapacity;

	private LookupConfig(boolean asyncEnabled, long asyncTimeoutMs, int asyncBufferCapacity) {
		this.asyncEnabled = asyncEnabled;
		this.asyncTimeoutMs = asyncTimeoutMs;
		this.asyncBufferCapacity = asyncBufferCapacity;
	}

	/**
	 * Returns true if async lookup is enabled.
	 */
	public boolean isAsyncEnabled() {
		return asyncEnabled;
	}

	/**
	 * Returns async timeout millisecond for the asynchronous operation to complete.
	 */
	public long getAsyncTimeoutMs() {
		return asyncTimeoutMs;
	}

	/**
	 * Returns the max number of async i/o operation that can be triggered.
	 */
	public int getAsyncBufferCapacity() {
		return asyncBufferCapacity;
	}

	/**
	 * Returns a new builder that builds a {@link LookupConfig}.
	 *
	 * <p>For example:
	 *
	 * <pre>
	 *     LookupConfig.builder()
	 *       .setAsyncEnabled(true)
	 *       .setAsyncBufferCapacity(1000)
	 *       .setAsyncTimeoutMs(30000)
	 *       .build();
	 * </pre>
	 */
	public static Builder builder() {
		return new Builder();
	}

	/**
	 * A builder used to build a new {@link LookupConfig}.
	 */
	public static class Builder {

		private boolean asyncEnabled = DEFAULT_ASYNC_ENABLED;
		private long asyncTimeoutMs = DEFAULT_ASYNC_TIMEOUT_MS;
		private int asyncBufferCapacity = DEFAULT_ASYNC_BUFFER_CAPACITY;

		public Builder setAsyncEnabled(boolean asyncEnabled) {
			this.asyncEnabled = asyncEnabled;
			return this;
		}

		public Builder setAsyncTimeoutMs(long timeoutMs) {
			this.asyncTimeoutMs = timeoutMs;
			return this;
		}

		public Builder setAsyncBufferCapacity(int bufferCapacity) {
			this.asyncBufferCapacity = bufferCapacity;
			return this;
		}

		public LookupConfig build() {
			return new LookupConfig(asyncEnabled, asyncTimeoutMs, asyncBufferCapacity);
		}

	}
}
