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

package org.apache.flink.kubernetes.highavailability;

import org.apache.flink.runtime.checkpoint.CheckpointStoreUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.kubernetes.utils.Constants.CHECKPOINT_ID_KEY_PREFIX;

/**
 * Singleton {@link CheckpointStoreUtil} implementation for Kubernetes.
 *
 */
public enum KubernetesCheckpointStoreUtil implements CheckpointStoreUtil {
	INSTANCE;

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesCheckpointStoreUtil.class);

	/**
	 * Convert a checkpoint id into a ConfigMap key.
	 *
	 * @param checkpointId to convert to the key
	 *
	 * @return key created from the given checkpoint id
	 */
	@Override
	public String checkpointIDToName(long checkpointId) {
		return CHECKPOINT_ID_KEY_PREFIX + String.format("%019d", checkpointId);
	}

	/**
	 * Converts a key in ConfigMap to the checkpoint id.
	 *
	 * @param key in ConfigMap
	 *
	 * @return Checkpoint id parsed from the key
	 */
	@Override
	public long nameToCheckpointID(String key) {
		try {
			return Long.parseLong(key.substring(CHECKPOINT_ID_KEY_PREFIX.length()));
		} catch (NumberFormatException e) {
			LOG.warn("Could not parse checkpoint id from {}. This indicates that the " +
				"checkpoint id to path conversion has changed.", key);

			return INVALID_CHECKPOINT_ID;
		}
	}
}
