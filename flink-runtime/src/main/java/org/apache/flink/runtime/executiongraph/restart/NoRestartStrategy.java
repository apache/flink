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

package org.apache.flink.runtime.executiongraph.restart;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;

import java.util.concurrent.CompletableFuture;

/**
 * Restart strategy which does not restart an {@link ExecutionGraph}.
 */
public class NoRestartStrategy implements RestartStrategy {

	@Override
	public boolean canRestart() {
		return false;
	}

	@Override
	public CompletableFuture<Void> restart(RestartCallback restarter, ScheduledExecutor executor) {
		throw new UnsupportedOperationException("NoRestartStrategy does not support restart.");
	}

	/**
	 * Creates a NoRestartStrategyFactory instance.
	 *
	 * @param configuration Configuration object which is ignored
	 * @return NoRestartStrategyFactory instance
	 */
	public static NoRestartStrategyFactory createFactory(Configuration configuration) {
		return new NoRestartStrategyFactory();
	}

	@Override
	public String toString() {
		return "NoRestartStrategy";
	}

	public static class NoRestartStrategyFactory extends RestartStrategyFactory {

		private static final long serialVersionUID = -1809462525812787862L;

		@Override
		public RestartStrategy createRestartStrategy() {
			return new NoRestartStrategy();
		}
	}
}
