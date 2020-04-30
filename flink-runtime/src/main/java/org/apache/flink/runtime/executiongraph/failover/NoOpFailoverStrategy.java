/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.executiongraph.failover;

import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;

import java.util.List;

/**
 * FailoverStrategy that does not do anything.
 */
public class NoOpFailoverStrategy extends FailoverStrategy {

	@Override
	public void onTaskFailure(final Execution taskExecution, final Throwable cause) {
	}

	@Override
	public void notifyNewVertices(final List<ExecutionJobVertex> newJobVerticesTopological) {
	}

	@Override
	public String getStrategyName() {
		return "NoOp failover strategy";
	}

	public static class Factory implements FailoverStrategy.Factory {

		@Override
		public FailoverStrategy create(final ExecutionGraph executionGraph) {
			return new NoOpFailoverStrategy();
		}
	}
}
