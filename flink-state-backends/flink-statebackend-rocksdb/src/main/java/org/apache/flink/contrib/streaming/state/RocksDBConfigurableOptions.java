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
import org.apache.flink.configuration.description.Description;

import java.io.Serializable;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.LinkElement.link;
import static org.rocksdb.CompactionStyle.FIFO;
import static org.rocksdb.CompactionStyle.LEVEL;
import static org.rocksdb.CompactionStyle.UNIVERSAL;

/**
 * This class contains the configuration options for the {@link DefaultConfigurableOptionsFactory}.
 *
 * <p>If nothing specified, RocksDB's options would be configured by {@link PredefinedOptions} and user-defined {@link OptionsFactory}.
 *
 * <p>If some options has been specifically configured, a corresponding {@link DefaultConfigurableOptionsFactory} would be created
 * and applied on top of {@link PredefinedOptions} except if a user-defined {@link OptionsFactory} overrides it.
 */
public class RocksDBConfigurableOptions implements Serializable {

	//--------------------------------------------------------------------------
	// Provided configurable DBOptions within Flink
	//--------------------------------------------------------------------------

	public static final ConfigOption<String> MAX_BACKGROUND_THREADS =
		key("state.backend.rocksdb.thread.num")
			.noDefaultValue()
			.withDescription("The maximum number of concurrent background flush and compaction jobs (per TaskManager). " +
				"RocksDB has default configuration as '1'.");

	public static final ConfigOption<String> MAX_OPEN_FILES =
		key("state.backend.rocksdb.files.open")
			.noDefaultValue()
			.withDescription("The maximum number of open files (per TaskManager) that can be used by the DB, '-1' means no limit. " +
				"RocksDB has default configuration as '5000'.");

	//--------------------------------------------------------------------------
	// Provided configurable ColumnFamilyOptions within Flink
	//--------------------------------------------------------------------------

	public static final ConfigOption<String> COMPACTION_STYLE =
		key("state.backend.rocksdb.compaction.style")
			.noDefaultValue()
			.withDescription(String.format("The specified compaction style for DB. Candidate compaction style is %s, %s or %s, " +
					"and RocksDB choose '%s' as default style.", LEVEL.name(), FIFO.name(), UNIVERSAL.name(),
				LEVEL.name()));

	public static final ConfigOption<String> USE_DYNAMIC_LEVEL_SIZE =
		key("state.backend.rocksdb.compaction.level.use-dynamic-size")
			.noDefaultValue()
			.withDescription(Description.builder().text("If true, RocksDB will pick target size of each level dynamically. From an empty DB, ")
				.text("RocksDB would make last level the base level, which means merging L0 data into the last level, ")
				.text("until it exceeds max_bytes_for_level_base. And then repeat this process for second last level and so on. ")
				.text("RocksDB has default configuration as 'false'. ")
				.text("For more information, please refer to %s",
					link("https://github.com/facebook/rocksdb/wiki/Leveled-Compaction#level_compaction_dynamic_level_bytes-is-true",
						"RocksDB's doc."))
				.build());

	public static final ConfigOption<String> TARGET_FILE_SIZE_BASE =
		key("state.backend.rocksdb.compaction.level.target-file-size-base")
			.noDefaultValue()
			.withDescription("The target file size for compaction, which determines a level-1 file size. " +
				"RocksDB has default configuration as '2MB'.");

	public static final ConfigOption<String> MAX_SIZE_LEVEL_BASE =
		key("state.backend.rocksdb.compaction.level.max-size-level-base")
			.noDefaultValue()
			.withDescription("The upper-bound of the total size of level base files in bytes. " +
				"RocksDB has default configuration as '10MB'.");

	public static final ConfigOption<String> WRITE_BUFFER_SIZE =
		key("state.backend.rocksdb.writebuffer.size")
			.noDefaultValue()
			.withDescription("The amount of data built up in memory (backed by an unsorted log on disk) " +
				"before converting to a sorted on-disk files. RocksDB has default writebuffer size as '64MB'.");

	public static final ConfigOption<String> MAX_WRITE_BUFFER_NUMBER =
		key("state.backend.rocksdb.writebuffer.count")
			.noDefaultValue()
			.withDescription("Tne maximum number of write buffers that are built up in memory. " +
				"RocksDB has default configuration as '2'.");

	public static final ConfigOption<String> MIN_WRITE_BUFFER_NUMBER_TO_MERGE =
		key("state.backend.rocksdb.writebuffer.number-to-merge")
			.noDefaultValue()
			.withDescription("The minimum number of write buffers that will be merged together before writing to storage. " +
				"RocksDB has default configuration as '1'.");

	public static final ConfigOption<String> BLOCK_SIZE =
		key("state.backend.rocksdb.block.blocksize")
			.noDefaultValue()
			.withDescription("The approximate size (in bytes) of user data packed per block. " +
				"RocksDB has default blocksize as '4KB'.");

	public static final ConfigOption<String> BLOCK_CACHE_SIZE =
		key("state.backend.rocksdb.block.cache-size")
			.noDefaultValue()
			.withDescription("The amount of the cache for data blocks in RocksDB. " +
				"RocksDB has default block-cache size as '8MB'.");

}
