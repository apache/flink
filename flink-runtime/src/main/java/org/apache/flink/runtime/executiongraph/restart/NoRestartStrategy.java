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
import org.apache.flink.runtime.executiongraph.ExecutionGraph;

/**
 * Restart strategy which does not restart an {@link ExecutionGraph}.
 */
public class NoRestartStrategy implements RestartStrategy {

	@Override
	public boolean canRestart() {
		return false;
	}

	@Override
	public void restart(ExecutionGraph executionGraph) {
		throw new RuntimeException("NoRestartStrategy does not support restart.");
	}

	/**
	 * Creates a NoRestartStrategy instance.
	 *
	 * @param configuration Configuration object which is ignored
	 * @return NoRestartStrategy instance
	 */
	public static NoRestartStrategy create(Configuration configuration) {
		return new NoRestartStrategy();
	}
}
