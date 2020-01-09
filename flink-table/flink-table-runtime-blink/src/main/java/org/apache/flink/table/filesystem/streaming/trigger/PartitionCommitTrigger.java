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

package org.apache.flink.table.filesystem.streaming.trigger;

import java.util.LinkedHashMap;

/**
 * Partition trigger strategy for committing.
 */
public interface PartitionCommitTrigger {

	/**
	 * Can commit this partition.
	 */
	boolean canCommit(LinkedHashMap<String, String> partitionSpec, long watermark);

	static PartitionCommitTrigger createTrigger(
			String type, String customClass, ClassLoader userClassLoader) {
		type = type == null ? "custom" : type;
		switch (type.toLowerCase()) {
			case "day":
				return new DayPartitionCommitTrigger();
			case "day-hour":
				return new DayHourPartitionCommitTrigger();
			case "custom":
				if (customClass == null) {
					return null;
				}
				try {
					return (PartitionCommitTrigger) userClassLoader.loadClass(customClass).newInstance();
				} catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
					throw new RuntimeException(
							"Can not new instance for custom class from " + customClass, e);
				}
			default:
				throw new UnsupportedOperationException("Unsupported type: " + type);
		}
	}
}
