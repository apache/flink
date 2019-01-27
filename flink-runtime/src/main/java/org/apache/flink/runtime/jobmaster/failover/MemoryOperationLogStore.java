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

package org.apache.flink.runtime.jobmaster.failover;

import org.apache.flink.annotation.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of {@link OperationLogStore} that store all {@link OperationLog}
 * in memory.
 */
public class MemoryOperationLogStore implements OperationLogStore {
	private static final Logger LOG = LoggerFactory.getLogger(MemoryOperationLogStore.class);

	private static List<OperationLog> operationLogs;

	@VisibleForTesting
	public static void disable() {
		operationLogs = null;
	}

	private int readIndex;

	MemoryOperationLogStore() {
	}

	public void start() {
		if (operationLogs == null) {
			operationLogs = new ArrayList<>();
		}
		readIndex = 0;
	}

	public void stop() {
		readIndex = 0;
	}

	public void clear() {
		if (operationLogs != null) {
			operationLogs.clear();
		}
		readIndex = 0;
		if (LOG.isDebugEnabled()) {
			LOG.debug("All operation logs in memory are cleared.");
		}

	}

	@Override
	public void writeOpLog(@Nonnull OperationLog opLog) {
		if (operationLogs == null) {
			throw new IllegalStateException("Cannot write OpLog into a store before starting it");
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Writing a operation log in memory.");
		}

		operationLogs.add(opLog);
	}

	@Override
	public OperationLog readOpLog() {
		if (operationLogs == null) {
			throw new IllegalStateException("Cannot read OpLog from a store before starting it");
		}
		if (readIndex < operationLogs.size()) {
			return operationLogs.get(readIndex++);
		} else {
			return null;
		}
	}
}

