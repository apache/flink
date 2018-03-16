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

import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;

/**
 * Strategy for {@link ExecutionGraph} restarts.
 */
public interface RestartStrategy {

	/**
	 * True if the restart strategy can be applied to restart the {@link ExecutionGraph}.
	 *
	 * @return true if restart is possible, otherwise false
	 */
	boolean canRestart();

	/**
	 * Called by the ExecutionGraph to eventually trigger a full recovery.
	 * The recovery must be triggered on the given callback object, and may be delayed
	 * with the help of the given scheduled executor.
	 *
	 * <p>The thread that calls this method is not supposed to block/sleep.
	 *
	 * @param restarter The hook to restart the ExecutionGraph
	 * @param executor An scheduled executor to delay the restart
	 */
	void restart(RestartCallback restarter, ScheduledExecutor executor);
}
