/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.filesystem.streaming;

import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;

import java.io.IOException;

/**
 * Table {@link RollingPolicy}, now it is a {@link CheckpointRollingPolicy}.
 * Because partition commit is hard to support false.
 */
public class TableRollingPolicy<T> extends CheckpointRollingPolicy<T, String> {

	private final long partSize;
	private final long rolloverInterval;

	public TableRollingPolicy(long partSize, long rolloverInterval) {
		this.partSize = partSize <= 0 ? Long.MAX_VALUE : partSize;
		this.rolloverInterval = rolloverInterval <= 0 ? Long.MAX_VALUE : rolloverInterval;
	}

	@Override
	public boolean shouldRollOnEvent(
			PartFileInfo<String> partFileState,
			T element) throws IOException {
		return partFileState.getSize() > partSize;
	}

	@Override
	public boolean shouldRollOnProcessingTime(
			PartFileInfo<String> partFileState,
			long currentTime) {
		return currentTime - partFileState.getCreationTime() >= rolloverInterval;
	}
}
