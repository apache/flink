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

package org.apache.flink.runtime.executiongraph.failover.flip1;

/**
 * Restart strategy which does not restart tasks when tasks fail.
 */
public enum  NoRestartBackoffTimeStrategy implements RestartBackoffTimeStrategy {
	INSTANCE;

	@Override
	public boolean canRestart() {
		return false;
	}

	@Override
	public long getBackoffTime() {
		return 0L;
	}

	@Override
	public void notifyFailure(final Throwable cause) {
		// nothing to do
	}

	@Override
	public String toString() {
		return "NoRestartBackoffTimeStrategy";
	}

	/**
	 * The factory for creating {@link NoRestartBackoffTimeStrategy}.
	 */
	public enum NoRestartBackoffTimeStrategyFactory implements Factory {
		INSTANCE;

		@Override
		public RestartBackoffTimeStrategy create() {
			return NoRestartBackoffTimeStrategy.INSTANCE;
		}

		@Override
		public String toString() {
			return "NoRestartBackoffTimeStrategyFactory";
		}
	}
}
