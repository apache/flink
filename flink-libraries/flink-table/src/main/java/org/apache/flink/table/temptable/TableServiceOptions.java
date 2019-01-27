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

package org.apache.flink.table.temptable;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;


/**
 * The set of configuration options relating to TableService.
 */
@PublicEvolving
public final class TableServiceOptions {

	/** Not intended to be instantiated. */
	private TableServiceOptions() {}

	/**
	 * Specify the table service class name.
	 * This class must have a default constructor.
	 */
	public static final ConfigOption<String> TABLE_SERVICE_CLASS_NAME =
		key("flink.service.table.service.class.name")
		.defaultValue(FlinkTableService.class.getCanonicalName())
		.withDescription("Specify the table service class name, this class must have a default constructor.");

	/**
	 * Specify the parallelism of TableService.
	 */
	public static final ConfigOption<Integer> TABLE_SERVICE_PARALLELISM =
		key("flink.service.table.service.parallelism")
		.defaultValue(1)
		.withDescription("Specify the parallelism of TableService.");

	/**
	 * Specify the heap memory of TableService.
	 */
	public static final ConfigOption<Integer> TABLE_SERVICE_HEAP_MEMORY_MB =
		key("flink.service.table.service.heap.memory.mb")
		.defaultValue(256)
		.withDescription("Specify the heap memory (in megabytes) of TableService.");

	/**
	 * Specify the direct memory of TableService.
	 */
	public static final ConfigOption<Integer> TABLE_SERVICE_DIRECT_MEMORY_MB =
		key("flink.service.table.service.direct.memory.mb")
		.defaultValue(64)
		.withDescription("Specify the direct memory (in megabytes) of TableService.");

	/**
	 * Specify the native memory of TableService.
	 */
	public static final ConfigOption<Integer> TABLE_SERVICE_NATIVE_MEMORY_MB =
		key("flink.service.table.service.native.memory.mb")
		.defaultValue(64)
		.withDescription("Specify the native memory (in megabytes) of TableService.");

	/**
	 * Specify the cpu cores of TableService.
	 */
	public static final ConfigOption<Double> TABLE_SERVICE_CPU_CORES =
		key("flink.service.table.service.cpu.cores")
		.defaultValue(0.1)
		.withDescription("Specify the cpu cores of TableService.");

	/**
	 * How many times the TableServiceClient will retry to check TableService's status.
	 * TableServiceClient will throw {@link TableServiceException} if the retries are exhausted.
	 */
	public static final ConfigOption<Integer> TABLE_SERVICE_READY_RETRY_TIMES =
		key("flink.service.table.service.ready.retry.times")
		.defaultValue(3)
		.withDescription("How many times the TableServiceClient will retry to check TableService's status.");

	/**
	 * How long the TableServiceClient will wait before the next retry.
	 */
	public static final ConfigOption<Long> TABLE_SERVICE_READY_RETRY_BACKOFF_MS =
		key("flink.service.table.service.ready.backoff.ms")
		.defaultValue(10000L)
		.withDescription("How long the TableServiceClient will wait before the next retry.");

	/**
	 * Specify the id of the TableService instance.
	 * This configuration should be used internally.
	 */
	public static final ConfigOption<String> TABLE_SERVICE_ID =
		key("flink.service.table.service.id")
		.noDefaultValue();

	/**
	 * Specify the root path for table storage.
	 * System.getProperty("user.dir") will be used if not specified.
	 */
	public static final ConfigOption<String> TABLE_SERVICE_STORAGE_ROOT_PATH =
		key("flink.service.table.service.storage.root.path")
		.noDefaultValue()
		.withDescription("Specify the root path for table storage, System.getProperty(\"user.dir\") will be used if not specified.");

	/**
	 * Specify the maximum size (in bytes) of a table partition segment file.
	 */
	public static final ConfigOption<Integer> TABLE_SERVICE_STORAGE_SEGMENT_MAX_SIZE =
		key("fink.service.table.service.storage.segment.max.size")
		.defaultValue(128 * 1024 * 1024)
		.withDescription("Specify the maximum size (in bytes) of a table partition segment file.");

	/**
	 * Specify the read buffer size for TableServiceClient.
	 */
	public static final ConfigOption<Integer> TABLE_SERVICE_CLIENT_READ_BUFFER_SIZE =
		key("flink.service.table.service.client.read.buffer.size")
		.defaultValue(4 * 1024 * 1024)
		.withDescription("Specify the read buffer size (in bytes) for TableServiceClient.");

	/**
	 * Specify the write buffer size for TableServiceClient.
	 */
	public static final ConfigOption<Integer> TABLE_SERVICE_CLIENT_WRITE_BUFFER_SIZE =
		key("flink.service.table.service.client.write.buffer.size")
		.defaultValue(4 * 1024 * 1024)
		.withDescription("Specify the write buffer size (in bytes) for TableServiceClient.");

}
