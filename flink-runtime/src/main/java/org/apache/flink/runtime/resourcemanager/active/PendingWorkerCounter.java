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

package org.apache.flink.runtime.resourcemanager.active;

import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for counting pending workers per {@link WorkerResourceSpec}.
 */
class PendingWorkerCounter {
	private final Map<WorkerResourceSpec, Integer> pendingWorkerNums;

	PendingWorkerCounter() {
		pendingWorkerNums = new HashMap<>();
	}

	int getTotalNum() {
		return pendingWorkerNums.values().stream().reduce(0, Integer::sum);
	}

	int getNum(final WorkerResourceSpec workerResourceSpec) {
		return pendingWorkerNums.getOrDefault(Preconditions.checkNotNull(workerResourceSpec), 0);
	}

	int increaseAndGet(final WorkerResourceSpec workerResourceSpec) {
		return pendingWorkerNums.compute(
				Preconditions.checkNotNull(workerResourceSpec),
				(ignored, num) -> num != null ? num + 1 : 1);
	}

	int decreaseAndGet(final WorkerResourceSpec workerResourceSpec) {
		final Integer newValue = pendingWorkerNums.compute(
				Preconditions.checkNotNull(workerResourceSpec),
				(ignored, num) -> {
					Preconditions.checkState(num != null && num > 0,
							"Cannot decrease, no pending worker of spec %s.", workerResourceSpec);
					return num == 1 ? null : num - 1;
				});
		return newValue != null ? newValue : 0;
	}
}
