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

package org.apache.flink.api.java.io.jdbc;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.api.java.io.jdbc.JDBCUpsertOutputFormat.DEFAULT_MAX_RETRY_TIMES;

/**
 * Options for {@link JDBCLookupFunction} and {@link JDBCAsyncLookupFunction}.
 */
public class JDBCLookupOptions implements Serializable {

	private final int maxPoolSize;
	private final int batchQuerySize;
	private final int threadPoolSize;
	private final long cacheMaxSize;
	private final long cacheExpireMs;
	private final int maxRetryTimes;

	private JDBCLookupOptions(
		int maxPoolSize, int batchQuerySize,
		int threadPoolSize, long cacheMaxSize,
		long cacheExpireMs, int maxRetryTimes) {
		this.maxPoolSize = maxPoolSize;
		this.batchQuerySize = batchQuerySize;
		this.threadPoolSize = threadPoolSize;
		this.cacheMaxSize = cacheMaxSize;
		this.cacheExpireMs = cacheExpireMs;
		this.maxRetryTimes = maxRetryTimes;
	}

	public int getMaxPoolSize() {
		return maxPoolSize;
	}

	public int getBatchQuerySize() {
		return batchQuerySize;
	}

	public int getThreadPoolSize() {
		return threadPoolSize;
	}

	public long getCacheMaxSize() {
		return cacheMaxSize;
	}

	public long getCacheExpireMs() {
		return cacheExpireMs;
	}

	public int getMaxRetryTimes() {
		return maxRetryTimes;
	}

	public static Builder builder() {
		return new Builder();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof JDBCLookupOptions) {
			JDBCLookupOptions options = (JDBCLookupOptions) o;
			return Objects.equals(maxPoolSize, options.maxPoolSize) &&
				Objects.equals(batchQuerySize, options.batchQuerySize) &&
				Objects.equals(threadPoolSize, options.threadPoolSize) &&
				Objects.equals(cacheMaxSize, options.cacheMaxSize) &&
				Objects.equals(cacheExpireMs, options.cacheExpireMs) &&
				Objects.equals(maxRetryTimes, options.maxRetryTimes);
		} else {
			return false;
		}
	}

	/**
	 * Builder of {@link JDBCLookupFunction} and {@link JDBCAsyncLookupFunction}.
	 */
	public static class Builder {

		private int maxPoolSize = 10;
		private int batchQuerySize = 1;
		private int threadPoolSize = 10;
		private long cacheMaxSize = -1L;
		private long cacheExpireMs = -1L;
		private int maxRetryTimes = DEFAULT_MAX_RETRY_TIMES;

		/**
		 * optional, max connection pool size, over this value, the old data will be eliminated.
		 */
		public Builder setMaxPoolSize(int maxPoolSize) {
			this.maxPoolSize = maxPoolSize;
			return this;
		}

		/**
		 * optional, async lookup thread pool size, over this value, the old data will be eliminated.
		 */
		public Builder setThreadPoolSize(int threadPoolSize) {
			this.threadPoolSize = threadPoolSize;
			return this;
		}

		/**
		 * optional, async lookup batch query size, over this value, the old data will be eliminated.
		 */
		public Builder setBatchQuerySize(int batchQuerySize) {
			this.batchQuerySize = batchQuerySize;
			return this;
		}

		/**
		 * optional, lookup cache max size, over this value, the old data will be eliminated.
		 */
		public Builder setCacheMaxSize(long cacheMaxSize) {
			this.cacheMaxSize = cacheMaxSize;
			return this;
		}

		/**
		 * optional, lookup cache expire mills, over this time, the old data will expire.
		 */
		public Builder setCacheExpireMs(long cacheExpireMs) {
			this.cacheExpireMs = cacheExpireMs;
			return this;
		}

		/**
		 * optional, max retry times for jdbc connector.
		 */
		public Builder setMaxRetryTimes(int maxRetryTimes) {
			this.maxRetryTimes = maxRetryTimes;
			return this;
		}

		public JDBCLookupOptions build() {
			return new JDBCLookupOptions(maxPoolSize, batchQuerySize,
				threadPoolSize, cacheMaxSize, cacheExpireMs, maxRetryTimes);
		}
	}
}
