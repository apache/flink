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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * Configuration options for the RocksDB backend.
 */
public class RockDBBackendOptions {

	/**
	 * Choice of implementation for priority queue state (e.g. timers).
	 */
	public static final ConfigOption<String> PRIORITY_QUEUE_STATE_TYPE = ConfigOptions
		.key("backend.rocksdb.priority_queue_state_type")
		.defaultValue(RocksDBStateBackend.PriorityQueueStateType.HEAP.name())
		.withDescription("This determines the implementation for the priority queue state (e.g. timers). Options are" +
			"either " + RocksDBStateBackend.PriorityQueueStateType.HEAP.name() + " (heap-based, default) or " +
			RocksDBStateBackend.PriorityQueueStateType.ROCKS.name() + " for in implementation based on RocksDB.");
}
