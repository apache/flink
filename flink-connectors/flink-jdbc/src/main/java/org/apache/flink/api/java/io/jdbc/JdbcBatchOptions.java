/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.java.io.jdbc;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;

/**
 * JDBC sink batch options.
 */
@PublicEvolving
public class JdbcBatchOptions implements Serializable {
	private static final int DEFAULT_INTERVAL_MILLIS = 0;
	private static final int DEFAULT_SIZE = 5000;

	private final long intervalMs;
	private final int size;

	private JdbcBatchOptions(long intervalMs, int size) {
		this.intervalMs = intervalMs;
		this.size = size;
	}

	public long getIntervalMs() {
		return intervalMs;
	}

	public int getSize() {
		return size;
	}

	public static JdbcBatchOptionsBuilder builder() {
		return new JdbcBatchOptionsBuilder();
	}

	public static JdbcBatchOptions defaults() {
		return builder().build();
	}

	/**
	 * JDBCBatchOptionsBuilder.
	 */
	public static final class JdbcBatchOptionsBuilder {
		private long intervalMs = DEFAULT_INTERVAL_MILLIS;
		private int size = DEFAULT_SIZE;

		public JdbcBatchOptionsBuilder withSize(int size) {
			this.size = size;
			return this;
		}

		public JdbcBatchOptionsBuilder withIntervalMs(long intervalMs) {
			this.intervalMs = intervalMs;
			return this;
		}

		public JdbcBatchOptions build() {
			return new JdbcBatchOptions(intervalMs, size);
		}
	}
}
