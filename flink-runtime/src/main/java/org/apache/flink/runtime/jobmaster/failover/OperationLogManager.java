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

import org.apache.flink.util.FlinkRuntimeException;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The manager for managing the recording and replaying of the {@link OperationLog}.
 */
public class OperationLogManager {

	private final OperationLogStore operationLogStore;

	private final Map<OperationLogType, Replayable> opLogTypeToReplayHandlers;

	private volatile boolean replaying = false;

	public OperationLogManager(OperationLogStore store) {
		this.operationLogStore = checkNotNull(store);
		opLogTypeToReplayHandlers = new HashMap<>(1);
	}

	/**
	 * Start the operation log manager.
	 */
	public void start() {
		operationLogStore.start();
	}

	/**
	 * Stop the operation log manager.
	 */
	public void stop() {
		operationLogStore.stop();
	}

	/**
	 * Clear all the logs in store.
	 */
	public void clear() {
		operationLogStore.clear();
		replaying = false;
	}

	/**
	 * Weather is replaying log.
	 */
	public boolean isReplaying() {
		return replaying;
	}

	/**
	 * Register the handler for the operation log type.
	 *
	 * @param type The log type
	 * @param logHandler The handler for the specified log type when replaying.
	 */
	public void registerLogHandler(OperationLogType type, Replayable logHandler) {
		opLogTypeToReplayHandlers.put(type, logHandler);
	}

	/**
	 * Write an operation log.
	 *
	 * @param opLog The operation log need to be record
	 */
	public void writeOpLog(OperationLog opLog){
		operationLogStore.writeOpLog(opLog);
	}

	/**
	 * Replay the operation logs that have been record.
	 */
	public void replay() {
		replaying = true;
		OperationLog opLog;
		while ((opLog = operationLogStore.readOpLog()) != null) {
			Replayable replayHandler = opLogTypeToReplayHandlers.get(opLog.getType());
			if (replayHandler != null) {
				replayHandler.replayOpLog(opLog);
			} else {
				throw new FlinkRuntimeException("Can not find replayer for log type " + opLog.getType());
			}
		}
		replaying = false;
	}
}
