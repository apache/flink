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

import static org.apache.flink.contrib.streaming.state.RocksDBStateBackend.PriorityQueueStateType.HEAP;
import static org.apache.flink.contrib.streaming.state.RocksDBStateBackend.PriorityQueueStateType.ROCKSDB;

/**
 * Configuration options for the RocksDB backend.
 */
public class RocksDBOptions {

	/** The local directory (on the TaskManager) where RocksDB puts its files. */
	public static final ConfigOption<String> LOCAL_DIRECTORIES = ConfigOptions
		.key("state.backend.rocksdb.localdir")
		.noDefaultValue()
		.withDeprecatedKeys("state.backend.rocksdb.checkpointdir")
		.withDescription("The local directory (on the TaskManager) where RocksDB puts its files.");

	/**
	 * Choice of timer service implementation.
	 */
	public static final ConfigOption<String> TIMER_SERVICE_FACTORY = ConfigOptions
		.key("state.backend.rocksdb.timer-service.factory")
		.defaultValue(HEAP.name())
		.withDescription(String.format("This determines the factory for timer service state implementation. Options " +
			"are either %s (heap-based, default) or %s for an implementation based on RocksDB .",
			HEAP.name(), ROCKSDB.name()));
}
