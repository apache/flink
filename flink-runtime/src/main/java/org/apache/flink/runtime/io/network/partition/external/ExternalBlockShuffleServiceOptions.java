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

package org.apache.flink.runtime.io.network.partition.external;

import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * The set of configuration options relating to ExternalBlockShuffleService settings.
 */
public class ExternalBlockShuffleServiceOptions {

	/**
	 * (Compulsory) A fixed port for flink shuffle service on each node.
	 */
	public static final ConfigOption<Integer> FLINK_SHUFFLE_SERVICE_PORT_KEY =
		key("flink.shuffle-service.port")
			.defaultValue(14572)
			.withDescription("The port of the shuffle service.");

	/**
	 * Direct memory limit for flink shuffle service, in MB.
	 * WARNING: MIN_BUFFER_NUMBER * MEMORY_SIZE_PER_BUFFER should not exceed direct memory limit.
	 */
	public static final ConfigOption<Integer> FLINK_SHUFFLE_SERVICE_DIRECT_MEMORY_LIMIT_IN_MB =
		key("flink.shuffle-service.direct-memory-limit-in-mb")
			.defaultValue(300)
			.withDescription("The direct memory consumed by the yarn shuffle service.");

	/**
	 * Heap memory limit for flink shuffle service, in MB.
	 * TODO Will be used in the future.
	 */
	public static final ConfigOption<Integer> FLINK_SHUFFLE_SERVICE_HEAP_MEMORY_LIMIT_IN_MB =
		key("flink.shuffle-service.heap-memory-limit-in-mb")
			.defaultValue(64)
			.withDescription("The heap memory consumed by the yarn shuffle service");


	/**
	 * Local directories to process shuffle data for flink shuffle service.
	 *
	 * Format:
	 * (1) Separate directories by commas.
	 * (2) (Optional) Disk type info could be given for a directory if there are several types
	 *     of disks and thus require specific settings. In this scenario, disk type field,
	 *     embraced by square brackets, could be placed ahead of a directory string.
	 *     Regex pattern: "^(\\[(\\w*)\\])?(.+)$" .
	 * (3) If disk type field is not suggested, treat this directory as default disk type.
	 *
	 * Example:
	 * (1) /dump1/local-dir/, /dump2/local-dir/
	 * (2) [SSD]/dump1/local-dir/, [HDD]/dump2/local-dir/, /dump3/local-dir
	 *
	 */
	public static final ConfigOption<String> LOCAL_DIRS =
		key("flink.shuffle-service.local-dirs")
			.defaultValue("")
			.withDescription("The list of local directories separated by comma. Each directory can be " +
				"configured with the disk type ([TYPE] directory,...) or without the disk type (directory, ...). " +
				"If not configured, the NodeManager local directories will be used and each directory will be treated as on HDD.");

	/**
	 * IO thread number for each disk type suggested by LOCAL_DIRS.
	 *
	 * Format:
	 * (1) Separate disk type configurations by commas.
	 * (2) In order to describe a disk type configuration, set disk type as key,
	 *     the corresponding io thread number as value, separated by a semicolon.
	 *
	 * Example:
	 *     SSD: 30, HDD: 4
	 *     SSD: 30, HDD: 4
	 */
	public static final ConfigOption<String> IO_THREAD_NUM_FOR_DISK_TYPE =
		key("flink.shuffle-service.io-thread-number-for-disk-type")
			.defaultValue("")
			.withDescription("The list of disk types and thread numbers. Each disk type and the " +
				"corresponding thread number is configured by TYPE_1: THREAD_1, TYPE_2: THREAD_2, ...");

	/**
	 * Default IO thread number for a directory if its disk type is not specified.
	 */
	public static final ConfigOption<Integer> DEFAULT_IO_THREAD_NUM_PER_DISK =
		key("flink.shuffle-service.default-io-thread-number-per-disk")
			.defaultValue(4)
			.withDescription("The thread number for the default HDD disk type.");

	/**
	 * Netty thread number for handling requests, used to set NettyConfig.NUM_THREADS_SERVER in netty.
	 * If it's not positive, use overall IO thread number as netty thread number.
	 */
	public static final ConfigOption<Integer> SERVER_THREAD_NUM =
		key("flink.shuffle-service.server-thread-number")
		.defaultValue(0)
		.withDescription("The number of netty IO threads. If it's not positive, " +
			"the thread number is equal to the overall IO thread number");

	/**
	 * The size of direct memory allocated for Netty threads.
	 */
	public static final ConfigOption<Integer> NETTY_MEMORY_IN_MB =
		key("flink.shuffle-service.netty-memory-in-mb")
			.defaultValue(0)
			.withDescription("The preferred number of netty IO threads. If it's positive, the netty memory size will be " +
				"min(configured value, 4M * flink.shuffle-service.server-thread-number), otherwise the netty memory size will be " +
				"min(1/2 * flink.shuffle-service.direct-memory-limit-in-mb, 4M * flink.shuffle-service.server-thread-number).");

	/**
	 * The memory size of one buffer, in bytes.
	 */
	public static final ConfigOption<Integer> MEMORY_SIZE_PER_BUFFER_IN_BYTES =
		key("flink.shuffle-service.memory-size-per-buffer-in-bytes")
			.defaultValue(32768)
			.withDescription("");

	/**
	 * The duration to retain a partition's data after its last modified time
	 * if this partition is unfinished and cannot be consumed, probably due to upstream write failure,
	 * in seconds.
	 */
	public static final ConfigOption<Long> WAIT_CREDIT_DELAY_IN_MS =
		key("flink.shuffle-service.wait-credit-delay-in-ms")
			.defaultValue(0L)
			.withDescription("");

	/**
	 * The interval to do disk scan periodically, in milliseconds.
	 */
	public static final ConfigOption<Long> DISK_SCAN_INTERVAL_IN_MS =
		key("flink.shuffle-service.disk-scan-interval-in-ms")
			.defaultValue(15000L)
			.withDescription("");

	/**
	 * The duration to retain a partition's data after it has been fully consumed, in seconds.
	 */
	public static final ConfigOption<Integer> CONSUMED_PARTITION_TTL_IN_SECONDS =
		key("flink.shuffle-service.consumed-partition-ttl-in-seconds")
			.defaultValue(60 * 60)
			.withDescription("The time interval to delete the fully consumed shuffle data directories " +
				"since they become inactive.");

	/**
	 * The duration to retain a partition's data after its last consumption if it hasn't been fully consumed,
	 * in seconds.
	 */
	public static final ConfigOption<Integer> PARTIAL_CONSUMED_PARTITION_TTL_IN_SECONDS =
		key("flink.shuffle-service.partial-consumed-partition-ttl-in-seconds")
			.defaultValue(60 * 60 * 12)
			.withDescription("The time interval to delete the partially consumed shuffle data directories " +
				"since they become inactive.");

	/**
	 * The duration to retain a partition's data after its last modified time
	 * if this partition is ready for consumption but hasn't been consumed yet, in seconds.
	 */
	public static final ConfigOption<Integer> UNCONSUMED_PARTITION_TTL_IN_SECONDS =
		key("flink.shuffle-service.unconsumed-partition-ttl-in-seconds")
			.defaultValue(60 * 60 * 12)
			.withDescription("TThe time interval to delete the unconsumed shuffle data directories " +
				"since they are ready to consume.");

	/**
	 * The duration to retain a partition's data after its last modified time
	 * if this partition is unfinished and cannot be consumed, probably due to upstream write failure,
	 * in seconds.
	 */
	public static final ConfigOption<Integer> UNFINISHED_PARTITION_TTL_IN_SECONDS =
		key("flink.shuffle-service.unfinished-partition-ttl-in-seconds")
			.defaultValue(60 * 60)
			.withDescription("The time interval to delete the writing shuffle data directories " +
				"since the last writing.");

	/**
	 * If this comparator is configured, disk IO thread will serve subpartition request based on
	 * the corresponding sort sequence. If this configuration is empty string, just use FIFO sequence.
	 * Users can customize their own comparator by implementing {@link java.util.Comparator}.
	 */
	public static final ConfigOption<String> SUBPARTITION_REQUEST_COMPARATOR_CLASS =
		key("flink.shuffle-service.subpartition-request-comparator-class")
		.defaultValue("org.apache.flink.runtime.io.network.partition.external.CreditBasedSubpartitionViewComparator")
		.withDescription("The comparator to decide the next subpartition to serve.");

	// ------------------------- Configurations for internal use ------------------------
	public static final ConfigOption<String> LOCAL_RESULT_PARTITION_RESOLVER_CLASS =
		key("flink.shuffle-service.internal.local-result-partition-resolver-class")
		.noDefaultValue();

	/** Not intended to be instantiated */
	private ExternalBlockShuffleServiceOptions() { }
}
