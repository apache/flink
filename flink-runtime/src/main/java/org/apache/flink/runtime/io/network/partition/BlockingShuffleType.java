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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.slf4j.Logger;

/**
 * This enumeration defines the shuffle type when the {@link ResultPartitionType} is
 * {@link ResultPartitionType#BLOCKING}. The BlockingShuffleType is configured by
 * {@link org.apache.flink.configuration.TaskManagerOptions#TASK_BLOCKING_SHUFFLE_TYPE}.
 */
public enum BlockingShuffleType {
	/**
	 * TM is the default shuffle type which refers to the internal taskmanager shuffle service.
	 */
	TM,

	/**
	 * Yarn refers to the external yarn shuffle service deployed on the yarn nodemanager.
	 */
	YARN;

	/**
	 * Utility method for retrieving shuffle type from current configuration.
	 * @param configuration current configuration
	 * @param logger omitting to log if logger is set null.
	 * @return configured blocking shuffle type
	 */
	static public BlockingShuffleType getBlockingShuffleTypeFromConfiguration(
		final Configuration configuration, Logger logger) {
		BlockingShuffleType shuffleType;
		try {
			shuffleType = BlockingShuffleType.valueOf(configuration.getString(
				TaskManagerOptions.TASK_BLOCKING_SHUFFLE_TYPE).toUpperCase());
		} catch (IllegalArgumentException e) {
			shuffleType = BlockingShuffleType.valueOf(TaskManagerOptions.TASK_BLOCKING_SHUFFLE_TYPE.defaultValue());
			if (logger != null) {
				logger.warn("The configured blocking shuffle type ["
					+ configuration.getString(TaskManagerOptions.TASK_BLOCKING_SHUFFLE_TYPE)
					+ "] is illegal, using default value: " + shuffleType, e);
			}
		}
		return shuffleType;
	}
}
