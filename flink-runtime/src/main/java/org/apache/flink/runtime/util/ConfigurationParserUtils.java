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

package org.apache.flink.runtime.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.util.MathUtils;

import static org.apache.flink.util.MathUtils.checkedDownCast;

/**
 * Utility class to extract related parameters from {@link Configuration} and to
 * sanity check them.
 */
public class ConfigurationParserUtils {

	/**
	 * Parses the configuration to get the number of slots and validates the value.
	 *
	 * @param configuration configuration object
	 * @return the number of slots in task manager
	 */
	public static int getSlot(Configuration configuration) {
		int slots = configuration.getInteger(TaskManagerOptions.NUM_TASK_SLOTS, 1);
		// we need this because many configs have been written with a "-1" entry
		if (slots == -1) {
			slots = 1;
		}

		ConfigurationParserUtils.checkConfigParameter(slots >= 1, slots, TaskManagerOptions.NUM_TASK_SLOTS.key(),
			"Number of task slots must be at least one.");

		return slots;
	}

	/**
	 * Validates a condition for a config parameter and displays a standard exception, if the
	 * the condition does not hold.
	 *
	 * @param condition             The condition that must hold. If the condition is false, an exception is thrown.
	 * @param parameter         The parameter value. Will be shown in the exception message.
	 * @param name              The name of the config parameter. Will be shown in the exception message.
	 * @param errorMessage  The optional custom error message to append to the exception message.
	 *
	 * @throws IllegalConfigurationException if the condition does not hold
	 */
	public static void checkConfigParameter(boolean condition, Object parameter, String name, String errorMessage)
		throws IllegalConfigurationException {
		if (!condition) {
			throw new IllegalConfigurationException("Invalid configuration value for " +
				name + " : " + parameter + " - " + errorMessage);
		}
	}

	/**
	 * Parses the configuration to get the page size and validates the value.
	 *
	 * @param configuration configuration object
	 * @return size of memory segment
	 */
	public static int getPageSize(Configuration configuration) {
		final int pageSize = checkedDownCast(
			configuration.get(TaskManagerOptions.MEMORY_SEGMENT_SIZE).getBytes());

		// check page size of for minimum size
		checkConfigParameter(
			pageSize >= MemoryManager.MIN_PAGE_SIZE,
			pageSize,
			TaskManagerOptions.MEMORY_SEGMENT_SIZE.key(),
			"Minimum memory segment size is " + MemoryManager.MIN_PAGE_SIZE);
		// check page size for power of two
		checkConfigParameter(
			MathUtils.isPowerOf2(pageSize),
			pageSize,
			TaskManagerOptions.MEMORY_SEGMENT_SIZE.key(),
			"Memory segment size must be a power of 2.");

		return pageSize;
	}
}
