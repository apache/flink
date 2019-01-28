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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.util.Preconditions;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.DBOptions;
import org.rocksdb.PlainTableConfig;
import org.rocksdb.TableFormatConfig;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.LinkElement.link;
import static org.rocksdb.CompactionStyle.FIFO;
import static org.rocksdb.CompactionStyle.LEVEL;
import static org.rocksdb.CompactionStyle.UNIVERSAL;

/**
 * Customize RocksDB with provided options.
 *
 * <p>If nothing specified, RocksDB's options would be configured by {@link PredefinedOptions} and {@link OptionsFactory}.
 *
 * <p>If some options has been specifically configured, they would be applied on top of {@link PredefinedOptions} but could be
 * overrode by {@link OptionsFactory}. The hierarchy of these three layers is to keep backward compatibility.
 */
public class RocksDBCustomizedOptions implements Serializable {

	//--------------------------------------------------------------------------
	// Provided configurable DBOptions within Flink
	//--------------------------------------------------------------------------

	public static final ConfigOption<String> MAX_BACKGROUND_THREADS =
		key("state.backend.rocksdb.thread.num")
			.noDefaultValue()
			.withDescription("The maximum number of concurrent background flush and compaction jobs. " +
				"RocksDB has default configuration as '1'.");

	public static final ConfigOption<String> MAX_OPEN_FILES =
		key("state.backend.rocksdb.files.open")
			.noDefaultValue()
			.withDescription("The maximum number of open files that can be used by the DB, '-1' means no limit. " +
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
				"before converting to a sorted on-disk files. RocksDB has default writebuffer size as '4MB'.");

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

	public static final ConfigOption<String> PIN_L0_FILTER_INDEX =
		key("state.backend.rocksdb.block.pin-l0-index-filter")
			.noDefaultValue()
			.withDescription("Indicating whether we'd like to pin L0 index/filter blocks to the block cache. " +
				"RocksDB has default configuration as 'false'.");

	public static final ConfigOption<String> OPTIMIZE_HIT =
		key("state.backend.rocksdb.optimize-filter-hits")
			.noDefaultValue()
			.withDescription("Optimize filter hits, allows rocksDB to not store filters for the last level, " +
				"i.e the largest level which contains data of the LSM store. " +
				"RocksDB has default configuration as 'false'.");

	public static final ConfigOption<String> CACHE_INDEX_FILTER =
		key("state.backend.rocksdb.block.cache-index-filter")
			.noDefaultValue()
			.withDescription(Description.builder()
				.text("Indicating whether we'd put index/filter blocks into the block cache, RocksDB has default configuration as 'false'.")
				.text(String.format(" If setting this value as true, please consider to set %s and %s as true. ",
					PIN_L0_FILTER_INDEX.key(), OPTIMIZE_HIT.key()))
				.text("For more information, please refer to %s",
					link("https://github.com/facebook/rocksdb/wiki/Block-Cache#caching-index-and-filter-blocks",
						"RocksDB's doc."))
				.build());

	private static final String[] CANDIDATE_CONFIGS = new String[]{
		// configurable DBOptions
		MAX_BACKGROUND_THREADS.key(),
		MAX_OPEN_FILES.key(),

		// configurable ColumnFamilyOptions
		COMPACTION_STYLE.key(),
		USE_DYNAMIC_LEVEL_SIZE.key(),
		TARGET_FILE_SIZE_BASE.key(),
		MAX_SIZE_LEVEL_BASE.key(),
		WRITE_BUFFER_SIZE.key(),
		MAX_WRITE_BUFFER_NUMBER.key(),
		MIN_WRITE_BUFFER_NUMBER_TO_MERGE.key(),
		BLOCK_SIZE.key(),
		BLOCK_CACHE_SIZE.key(),
		PIN_L0_FILTER_INDEX.key(),
		OPTIMIZE_HIT.key(),
		CACHE_INDEX_FILTER.key()
	};

	private static final Set<String> POSITIVE_INT_CONFIG_SET = new HashSet<>(Arrays.asList(
		MAX_BACKGROUND_THREADS.key(),
		MAX_WRITE_BUFFER_NUMBER.key(),
		MIN_WRITE_BUFFER_NUMBER_TO_MERGE.key()
	));

	private static final Set<String> SIZE_CONFIG_SET = new HashSet<>(Arrays.asList(
		TARGET_FILE_SIZE_BASE.key(),
		MAX_SIZE_LEVEL_BASE.key(),
		WRITE_BUFFER_SIZE.key(),
		BLOCK_SIZE.key(),
		BLOCK_CACHE_SIZE.key()
	));

	private static final Set<String> BOOLEAN_CONFIG_SET = new HashSet<>(Arrays.asList(
		USE_DYNAMIC_LEVEL_SIZE.key(),
		PIN_L0_FILTER_INDEX.key(),
		OPTIMIZE_HIT.key(),
		CACHE_INDEX_FILTER.key()
	));

	private static final Set<String> COMPACTION_STYLE_SET = Arrays.stream(CompactionStyle.values())
		.map(c -> c.name().toLowerCase()).collect(Collectors.toSet());

	private final Map<String, String> configuredOptions = new HashMap<>();

	public RocksDBCustomizedOptions() {
		// nothing to do
	}

	/**
	 * Returns the new {@link RocksDBCustomizedOptions options} which combine current and the given customized options.
	 * For the conflicted options, they are overrode by the given customized options.
	 *
	 * @param givenCustomizedOptions The customized options to combine and override current options.
	 * @return The new customized options which combine current and the given customized options.
	 */
	public RocksDBCustomizedOptions overrideBy(RocksDBCustomizedOptions givenCustomizedOptions) {
		this.configuredOptions.putAll(givenCustomizedOptions.configuredOptions);
		return givenCustomizedOptions;
	}

	public DBOptions getDBOptions(DBOptions currentOptions) {

		if (isOptionConfigured(MAX_BACKGROUND_THREADS)) {
			currentOptions.setIncreaseParallelism(getMaxBackgroundThreads());
		}

		if (isOptionConfigured(MAX_OPEN_FILES)) {
			currentOptions.setMaxOpenFiles(getMaxOpenFiles());
		}

		return currentOptions;
	}

	public ColumnFamilyOptions getColumnOptions(ColumnFamilyOptions currentOptions) {
		if (isOptionConfigured(COMPACTION_STYLE)) {
			currentOptions.setCompactionStyle(getCompactionStyle());
		}

		if (isOptionConfigured(USE_DYNAMIC_LEVEL_SIZE)) {
			currentOptions.setLevelCompactionDynamicLevelBytes(getUseDynamicLevelSize());
		}

		if (isOptionConfigured(TARGET_FILE_SIZE_BASE)) {
			currentOptions.setTargetFileSizeBase(getTargetFileSizeBase());
		}

		if (isOptionConfigured(MAX_SIZE_LEVEL_BASE)) {
			currentOptions.setMaxBytesForLevelBase(getMaxSizeLevelBase());
		}

		if (isOptionConfigured(WRITE_BUFFER_SIZE)) {
			currentOptions.setWriteBufferSize(getWriteBufferSize());
		}

		if (isOptionConfigured(MAX_WRITE_BUFFER_NUMBER)) {
			currentOptions.setMaxWriteBufferNumber(getMaxWriteBufferNumber());
		}

		if (isOptionConfigured(MIN_WRITE_BUFFER_NUMBER_TO_MERGE)) {
			currentOptions.setMinWriteBufferNumberToMerge(getMinWriteBufferNumberToMerge());
		}

		if (isOptionConfigured(OPTIMIZE_HIT)) {
			currentOptions.setOptimizeFiltersForHits(getOptimizeFiltersForHits());
		}

		TableFormatConfig tableFormatConfig = currentOptions.tableFormatConfig();

		BlockBasedTableConfig blockBasedTableConfig;
		if (tableFormatConfig == null) {
			blockBasedTableConfig = new BlockBasedTableConfig();
		} else {
			if (tableFormatConfig instanceof PlainTableConfig) {
				// if the table format config is PlainTableConfig, we just return current column-family options
				return currentOptions;
			} else {
				blockBasedTableConfig = (BlockBasedTableConfig) tableFormatConfig;
			}
		}

		if (isOptionConfigured(BLOCK_SIZE)) {
			blockBasedTableConfig.setBlockSize(getBlockSize());
		}

		if (isOptionConfigured(BLOCK_CACHE_SIZE)) {
			blockBasedTableConfig.setBlockCacheSize(getBlockCacheSize());
		}

		if (isOptionConfigured(PIN_L0_FILTER_INDEX)) {
			blockBasedTableConfig.setPinL0FilterAndIndexBlocksInCache(getPinL0FilterIndex());
		}

		if (isOptionConfigured(CACHE_INDEX_FILTER)) {
			blockBasedTableConfig.setCacheIndexAndFilterBlocks(getCacheIndexAndFilterBlocks());
		}

		return currentOptions.setTableFormatConfig(blockBasedTableConfig);
	}

	public Map<String, String> getConfiguredOptions() {
		return new HashMap<>(configuredOptions);
	}

	private boolean isOptionConfigured(ConfigOption configOption) {
		return configuredOptions.containsKey(configOption.key());
	}

	//--------------------------------------------------------------------------
	// Maximum number of concurrent background flush and compaction threads
	//--------------------------------------------------------------------------

	private int getMaxBackgroundThreads() {
		return Integer.parseInt(getInternal(MAX_BACKGROUND_THREADS.key()));
	}

	public RocksDBCustomizedOptions setMaxBackgroundThreads(int totalThreadCount) {
		Preconditions.checkArgument(totalThreadCount > 0);
		configuredOptions.put(MAX_BACKGROUND_THREADS.key(), String.valueOf(totalThreadCount));
		return this;
	}

	//--------------------------------------------------------------------------
	// Maximum number of open files
	//--------------------------------------------------------------------------

	private int getMaxOpenFiles() {
		return Integer.parseInt(getInternal(MAX_OPEN_FILES.key()));
	}

	public RocksDBCustomizedOptions setMaxOpenFiles(int maxOpenFiles) {
		configuredOptions.put(MAX_OPEN_FILES.key(), String.valueOf(maxOpenFiles));
		return this;
	}

	//--------------------------------------------------------------------------
	// The style of compaction for DB.
	//--------------------------------------------------------------------------

	private CompactionStyle getCompactionStyle() {
		return CompactionStyle.valueOf(getInternal(COMPACTION_STYLE.key()).toUpperCase());
	}

	public RocksDBCustomizedOptions setCompactionStyle(CompactionStyle compactionStyle) {
		setInternal(COMPACTION_STYLE.key(), compactionStyle.name());
		return this;
	}

	//--------------------------------------------------------------------------
	// Whether to configure RocksDB to pick target size of each level dynamically.
	//--------------------------------------------------------------------------

	private boolean getUseDynamicLevelSize() {
		return getInternal(USE_DYNAMIC_LEVEL_SIZE.key()).compareToIgnoreCase("false") != 0;
	}

	public RocksDBCustomizedOptions setUseDynamicLevelSize(boolean value) {
		configuredOptions.put(USE_DYNAMIC_LEVEL_SIZE.key(), value ? "true" : "false");
		return this;
	}

	//--------------------------------------------------------------------------
	// The target file size for compaction, i.e., the per-file size for level-1
	//--------------------------------------------------------------------------

	private long getTargetFileSizeBase() {
		return MemorySize.parseBytes(getInternal(TARGET_FILE_SIZE_BASE.key()));
	}

	public RocksDBCustomizedOptions setTargetFileSizeBase(String targetFileSizeBase) {
		Preconditions.checkArgument(MemorySize.parseBytes(targetFileSizeBase) > 0,
			"Invalid configuration " + targetFileSizeBase + " for target file size base.");
		setInternal(TARGET_FILE_SIZE_BASE.key(), targetFileSizeBase);
		return this;
	}


	//--------------------------------------------------------------------------
	// Maximum total data size for a level, i.e., the max total size for level-1
	//--------------------------------------------------------------------------

	private long getMaxSizeLevelBase() {
		return MemorySize.parseBytes(getInternal(MAX_SIZE_LEVEL_BASE.key()));
	}

	public RocksDBCustomizedOptions setMaxSizeLevelBase(String maxSizeLevelBase) {
		Preconditions.checkArgument(MemorySize.parseBytes(maxSizeLevelBase) > 0,
			"Invalid configuration " + maxSizeLevelBase + " for max size of level base.");
		setInternal(MAX_SIZE_LEVEL_BASE.key(), maxSizeLevelBase);
		return this;
	}

	//--------------------------------------------------------------------------
	// Amount of data to build up in memory (backed by an unsorted log on disk)
	// before converting to a sorted on-disk file. Larger values increase
	// performance, especially during bulk loads.
	//--------------------------------------------------------------------------

	private long getWriteBufferSize() {
		return MemorySize.parseBytes(getInternal(WRITE_BUFFER_SIZE.key()));
	}

	public RocksDBCustomizedOptions setWriteBufferSize(String writeBufferSize) {
		Preconditions.checkArgument(MemorySize.parseBytes(writeBufferSize) > 0,
			"Invalid configuration " + writeBufferSize + " for write-buffer size.");

		setInternal(WRITE_BUFFER_SIZE.key(), writeBufferSize);
		return this;
	}

	//--------------------------------------------------------------------------
	// The maximum number of write buffers that are built up in memory.
	//--------------------------------------------------------------------------

	private int getMaxWriteBufferNumber() {
		return Integer.parseInt(getInternal(MAX_WRITE_BUFFER_NUMBER.key()));
	}

	public RocksDBCustomizedOptions setMaxWriteBufferNumber(int writeBufferNumber) {
		Preconditions.checkArgument(writeBufferNumber > 0,
			"Invalid configuration " + writeBufferNumber + " for max write-buffer number.");
		setInternal(MAX_WRITE_BUFFER_NUMBER.key(), Integer.toString(writeBufferNumber));
		return this;
	}

	//--------------------------------------------------------------------------
	// The minimum number that will be merged together before writing to storage
	//--------------------------------------------------------------------------

	private int getMinWriteBufferNumberToMerge() {
		return Integer.parseInt(getInternal(MIN_WRITE_BUFFER_NUMBER_TO_MERGE.key()));
	}

	public RocksDBCustomizedOptions setMinWriteBufferNumberToMerge(int writeBufferNumber) {
		Preconditions.checkArgument(writeBufferNumber > 0,
			"Invalid configuration " + writeBufferNumber + " for min write-buffer number to merge.");
		setInternal(MIN_WRITE_BUFFER_NUMBER_TO_MERGE.key(), Integer.toString(writeBufferNumber));
		return this;
	}

	//--------------------------------------------------------------------------
	// Approximate size of user data packed per block. Note that the block size
	// specified here corresponds to uncompressed data. The actual size of the
	// unit read from disk may be smaller if compression is enabled
	//--------------------------------------------------------------------------

	private long getBlockSize() {
		return MemorySize.parseBytes(getInternal(BLOCK_SIZE.key()));
	}

	public RocksDBCustomizedOptions setBlockSize(String blockSize) {
		Preconditions.checkArgument(MemorySize.parseBytes(blockSize) > 0,
			"Invalid configuration " + blockSize + " for block size.");
		setInternal(BLOCK_SIZE.key(), blockSize);
		return this;
	}

	//--------------------------------------------------------------------------
	// The amount of the cache for data blocks in RocksDB
	//--------------------------------------------------------------------------

	private long getBlockCacheSize() {
		return MemorySize.parseBytes(getInternal(BLOCK_CACHE_SIZE.key()));
	}

	public RocksDBCustomizedOptions setBlockCacheSize(String blockCacheSize) {
		Preconditions.checkArgument(MemorySize.parseBytes(blockCacheSize) > 0,
			"Invalid configuration " + blockCacheSize + " for block cache size.");
		setInternal(BLOCK_CACHE_SIZE.key(), blockCacheSize);

		return this;
	}

	//--------------------------------------------------------------------------
	// Whether optimize filters for hits
	//--------------------------------------------------------------------------

	private boolean getPinL0FilterIndex() {
		return getInternal(PIN_L0_FILTER_INDEX.key()).compareToIgnoreCase("false") != 0;
	}

	public RocksDBCustomizedOptions setPinL0FilterIndex(boolean value) {
		setInternal(PIN_L0_FILTER_INDEX.key(), value ? "true" : "false");
		return this;
	}

	//--------------------------------------------------------------------------
	// Whether optimize filters for hits
	//--------------------------------------------------------------------------

	private boolean getOptimizeFiltersForHits() {
		return getInternal(OPTIMIZE_HIT.key()).compareToIgnoreCase("false") != 0;
	}

	public RocksDBCustomizedOptions setOptimizeFiltersForHits(boolean value) {
		setInternal(OPTIMIZE_HIT.key(), value ? "true" : "false");
		return this;
	}

	//--------------------------------------------------------------------------
	// Cache index and filter blocks in block cache
	//--------------------------------------------------------------------------

	public boolean getCacheIndexAndFilterBlocks() {
		return getInternal(CACHE_INDEX_FILTER.key()).compareToIgnoreCase("false") != 0;
	}

	public RocksDBCustomizedOptions setCacheIndexAndFilterBlocks(boolean value) {
		configuredOptions.put(CACHE_INDEX_FILTER.key(), value ? "true" : "false");
		return this;
	}

	/**
	 * Creates a {@link RocksDBCustomizedOptions} instance from a {@link Configuration}.
	 *
	 * @param configuration Configuration to be used for the RocksDBCustomizedOptions creation
	 * @return A RocksDBCustomizedOptions instance created from the given configuration
	 */
	public static RocksDBCustomizedOptions fromConfig(Configuration configuration) {
		RocksDBCustomizedOptions customizedOptions = new RocksDBCustomizedOptions();
		for (String key : CANDIDATE_CONFIGS) {
			String newValue = configuration.getString(key, null);

			if (newValue != null) {
				if (checkArgumentValid(key, newValue)) {
					customizedOptions.configuredOptions.put(key, newValue);
				}
			}
		}
		return customizedOptions;
	}

	/**
	 * Sets the configuration with (key, value) if the key is predefined, otherwise throws IllegalArgumentException.
	 *
	 * @param key The configuration key, if key is not predefined, throws IllegalArgumentException out.
	 * @param value The configuration value.
	 */
	private void setInternal(String key, String value) {
		Preconditions.checkArgument(value != null && !value.isEmpty(),
			"The configuration value must not be empty.");

		configuredOptions.put(key, value);
	}

	/**
	 * Returns the value in string format with the given key.
	 *
	 * @param key The configuration-key to query in string format.
	 */
	private String getInternal(String key) {
		Preconditions.checkArgument(configuredOptions.containsKey(key),
			"The configuration " + key + " has not been configured.");

		return configuredOptions.get(key);
	}

	/**
	 * Helper method to check whether the (key,value) is valid through given configuration and returns the formatted value.
	 *
	 * @param key The configuration key which is configurable in {@link RocksDBCustomizedOptions}.
	 * @param value The value within given configuration.
	 *
	 * @return whether the given key and value in string format is legal.
	 */
	private static boolean checkArgumentValid(String key, String value) {
		if (POSITIVE_INT_CONFIG_SET.contains(key)) {

			Preconditions.checkArgument(Integer.parseInt(value) > 0,
				"Configured value for key: " + key + " must be larger than 0.");
		} else if (SIZE_CONFIG_SET.contains(key)) {

			Preconditions.checkArgument(MemorySize.parseBytes(value) > 0,
				"Configured size for key" + key + " must be larger than 0.");
		} else if (BOOLEAN_CONFIG_SET.contains(key)) {

			Preconditions.checkArgument("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value),
				"The configured boolean value: " + value + " for key: " + key + " is illegal.");
		} else if (key.equals(COMPACTION_STYLE.key())) {
			value = value.toLowerCase();
			Preconditions.checkArgument(COMPACTION_STYLE_SET.contains(value),
				"Compression type: " + value + " is not recognized with legal types: " + COMPACTION_STYLE_SET.stream().collect(Collectors.joining(", ")));
		}
		return true;
	}

}
