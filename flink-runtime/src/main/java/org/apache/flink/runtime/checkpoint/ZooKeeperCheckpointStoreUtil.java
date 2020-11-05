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

package org.apache.flink.runtime.checkpoint;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Singleton {@link CheckpointStoreUtil} implementation for ZooKeeper.
 *
 */
public enum  ZooKeeperCheckpointStoreUtil implements CheckpointStoreUtil {
	INSTANCE;

	private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperCheckpointStoreUtil.class);

	/**
	 * Convert a checkpoint id into a ZooKeeper path.
	 *
	 * @param checkpointId to convert to the path
	 * @return Path created from the given checkpoint id
	 */
	@Override
	public String checkpointIDToName(long checkpointId) {
		return String.format("/%019d", checkpointId);
	}

	/**
	 * Converts a path to the checkpoint id.
	 *
	 * @param path in ZooKeeper
	 * @return Checkpoint id parsed from the path
	 */
	@Override
	public long nameToCheckpointID(String path) {
		try {
			String numberString;

			// check if we have a leading slash
			if ('/' == path.charAt(0)) {
				numberString = path.substring(1);
			} else {
				numberString = path;
			}
			return Long.parseLong(numberString);
		} catch (NumberFormatException e) {
			LOG.warn("Could not parse checkpoint id from {}. This indicates that the " +
				"checkpoint id to path conversion has changed.", path);

			return INVALID_CHECKPOINT_ID;
		}
	}
}
