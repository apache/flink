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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.util.TestExecutors;
import org.junit.Test;
import org.mockito.Mockito;
import scala.concurrent.ExecutionContext$;

public class FixedDelayRestartStrategyTest {

	@Test
	public void testFixedDelayRestartStrategy() {
		int numberRestarts = 10;
		long restartDelay = 10;

		FixedDelayRestartStrategy fixedDelayRestartStrategy = new FixedDelayRestartStrategy(
			numberRestarts,
			restartDelay);

		ExecutionGraph executionGraph = mock(ExecutionGraph.class);
		when(executionGraph.getFutureExecutionContext())
			.thenReturn(ExecutionContext$.MODULE$.fromExecutor(TestExecutors.directExecutor()));

		while(fixedDelayRestartStrategy.canRestart()) {
			fixedDelayRestartStrategy.restart(executionGraph);
		}

		Mockito.verify(executionGraph, Mockito.times(numberRestarts)).restart();
	}
}
