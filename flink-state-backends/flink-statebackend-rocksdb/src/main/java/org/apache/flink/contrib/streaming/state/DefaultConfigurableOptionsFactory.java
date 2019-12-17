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
import org.apache.flink.util.Preconditions;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.DBOptions;
import org.rocksdb.PlainTableConfig;
import org.rocksdb.TableFormatConfig;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.contrib.streaming.state.RocksDBConfigurableOptions.BLOCK_CACHE_SIZE;
import static org.apache.flink.contrib.streaming.state.RocksDBConfigurableOptions.BLOCK_SIZE;
import static org.apache.flink.contrib.streaming.state.RocksDBConfigurableOptions.COMPACTION_STYLE;
import static org.apache.flink.contrib.streaming.state.RocksDBConfigurableOptions.MAX_BACKGROUND_THREADS;
import static org.apache.flink.contrib.streaming.state.RocksDBConfigurableOptions.MAX_OPEN_FILES;
import static org.apache.flink.contrib.streaming.state.RocksDBConfigurableOptions.MAX_SIZE_LEVEL_BASE;
import static org.apache.flink.contrib.streaming.state.RocksDBConfigurableOptions.MAX_WRITE_BUFFER_NUMBER;
import static org.apache.flink.contrib.streaming.state.RocksDBConfigurableOptions.MIN_WRITE_BUFFER_NUMBER_TO_MERGE;
import static org.apache.flink.contrib.streaming.state.RocksDBConfigurableOptions.TARGET_FILE_SIZE_BASE;
import static org.apache.flink.contrib.streaming.state.RocksDBConfigurableOptions.USE_DYNAMIC_LEVEL_SIZE;
import static org.apache.flink.contrib.streaming.state.RocksDBConfigurableOptions.WRITE_BUFFER_SIZE;

/**
 * An implementation of {@link ConfigurableRocksDBOptionsFactory} using options provided by {@link RocksDBConfigurableOptions}.
 * It acts as the default options factory within {@link RocksDBStateBackend} if the user did not define a {@link RocksDBOptionsFactory}.
 */
public class DefaultConfigurableOptionsFactory implements ConfigurableRocksDBOptionsFactory {

	private static final long serialVersionUID = 1L;

	private final Map<String, String> configuredOptions = new HashMap<>();

	@Override
	public DBOptions createDBOptions(DBOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
		if (isOptionConfigured(MAX_BACKGROUND_THREADS)) {
			currentOptions.setIncreaseParallelism(getMaxBackgroundThreads());
		}

		if (isOptionConfigured(MAX_OPEN_FILES)) {
			currentOptions.setMaxOpenFiles(getMaxOpenFiles());
		}

		return currentOptions;
	}

	@Override
	public ColumnFamilyOptions createColumnOptions(ColumnFamilyOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
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

		return currentOptions.setTableFormatConfig(blockBasedTableConfig);
	}

	public Map<String, String> getConfiguredOptions() {
		return new HashMap<>(configuredOptions);
	}

	private boolean isOptionConfigured(ConfigOption<?> configOption) {
		return configuredOptions.containsKey(configOption.key());
	}

	//--------------------------------------------------------------------------
	// Maximum number of concurrent background flush and compaction threads
	//--------------------------------------------------------------------------

	private int getMaxBackgroundThreads() {
		return Integer.parseInt(getInternal(MAX_BACKGROUND_THREADS.key()));
	}

	public DefaultConfigurableOptionsFactory setMaxBackgroundThreads(int totalThreadCount) {
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

	public DefaultConfigurableOptionsFactory setMaxOpenFiles(int maxOpenFiles) {
		configuredOptions.put(MAX_OPEN_FILES.key(), String.valueOf(maxOpenFiles));
		return this;
	}

	//--------------------------------------------------------------------------
	// The style of compaction for DB.
	//--------------------------------------------------------------------------

	private CompactionStyle getCompactionStyle() {
		return CompactionStyle.valueOf(getInternal(COMPACTION_STYLE.key()).toUpperCase());
	}

	public DefaultConfigurableOptionsFactory setCompactionStyle(CompactionStyle compactionStyle) {
		setInternal(COMPACTION_STYLE.key(), compactionStyle.name());
		return this;
	}

	//--------------------------------------------------------------------------
	// Whether to configure RocksDB to pick target size of each level dynamically.
	//--------------------------------------------------------------------------

	private boolean getUseDynamicLevelSize() {
		return getInternal(USE_DYNAMIC_LEVEL_SIZE.key()).compareToIgnoreCase("false") != 0;
	}

	public DefaultConfigurableOptionsFactory setUseDynamicLevelSize(boolean value) {
		configuredOptions.put(USE_DYNAMIC_LEVEL_SIZE.key(), value ? "true" : "false");
		return this;
	}

	//--------------------------------------------------------------------------
	// The target file size for compaction, i.e., the per-file size for level-1
	//--------------------------------------------------------------------------

	private long getTargetFileSizeBase() {
		return MemorySize.parseBytes(getInternal(TARGET_FILE_SIZE_BASE.key()));
	}

	public DefaultConfigurableOptionsFactory setTargetFileSizeBase(String targetFileSizeBase) {
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

	public DefaultConfigurableOptionsFactory setMaxSizeLevelBase(String maxSizeLevelBase) {
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

	public DefaultConfigurableOptionsFactory setWriteBufferSize(String writeBufferSize) {
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

	public DefaultConfigurableOptionsFactory setMaxWriteBufferNumber(int writeBufferNumber) {
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

	public DefaultConfigurableOptionsFactory setMinWriteBufferNumberToMerge(int writeBufferNumber) {
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

	public DefaultConfigurableOptionsFactory setBlockSize(String blockSize) {
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

	public DefaultConfigurableOptionsFactory setBlockCacheSize(String blockCacheSize) {
		Preconditions.checkArgument(MemorySize.parseBytes(blockCacheSize) > 0,
			"Invalid configuration " + blockCacheSize + " for block cache size.");
		setInternal(BLOCK_CACHE_SIZE.key(), blockCacheSize);

		return this;
	}

	private static final ConfigOption<?>[] CANDIDATE_CONFIGS = new ConfigOption<?>[] {
		// configurable DBOptions
		MAX_BACKGROUND_THREADS,
		MAX_OPEN_FILES,

		// configurable ColumnFamilyOptions
		COMPACTION_STYLE,
		USE_DYNAMIC_LEVEL_SIZE,
		TARGET_FILE_SIZE_BASE,
		MAX_SIZE_LEVEL_BASE,
		WRITE_BUFFER_SIZE,
		MAX_WRITE_BUFFER_NUMBER,
		MIN_WRITE_BUFFER_NUMBER_TO_MERGE,
		BLOCK_SIZE,
		BLOCK_CACHE_SIZE
	};

	private static final Set<ConfigOption<?>> POSITIVE_INT_CONFIG_SET = new HashSet<>(Arrays.asList(
		MAX_BACKGROUND_THREADS,
		MAX_WRITE_BUFFER_NUMBER,
		MIN_WRITE_BUFFER_NUMBER_TO_MERGE
	));

	private static final Set<ConfigOption<?>> SIZE_CONFIG_SET = new HashSet<>(Arrays.asList(
		TARGET_FILE_SIZE_BASE,
		MAX_SIZE_LEVEL_BASE,
		WRITE_BUFFER_SIZE,
		BLOCK_SIZE,
		BLOCK_CACHE_SIZE
	));

	/**
	 * Creates a {@link DefaultConfigurableOptionsFactory} instance from a {@link Configuration}.
	 *
	 * <p>If no options within {@link RocksDBConfigurableOptions} has ever been configured,
	 * the created OptionsFactory would not override anything defined in {@link PredefinedOptions}.
	 *
	 * @param configuration Configuration to be used for the ConfigurableOptionsFactory creation
	 * @return A ConfigurableOptionsFactory created from the given configuration
	 */
	@Override
	public DefaultConfigurableOptionsFactory configure(Configuration configuration) {
		for (ConfigOption<?> option : CANDIDATE_CONFIGS) {
			Optional<?> newValue = configuration.getOptional(option);

			if (newValue.isPresent()) {
				checkArgumentValid(option, newValue.get());
				this.configuredOptions.put(option.key(), newValue.get().toString());
			}
		}
		return this;
	}

	@Override
	public String toString() {
		return "DefaultConfigurableOptionsFactory{" +
			"configuredOptions=" + configuredOptions +
			'}';
	}

	/**
	 * Helper method to check whether the (key,value) is valid through given configuration and returns the formatted value.
	 *
	 * @param option The configuration key which is configurable in {@link RocksDBConfigurableOptions}.
	 * @param value The value within given configuration.
	 */
	private static void checkArgumentValid(ConfigOption<?> option, Object value) {
		final String key = option.key();

		if (POSITIVE_INT_CONFIG_SET.contains(option)) {
			Preconditions.checkArgument((Integer) value  > 0,
				"Configured value for key: " + key + " must be larger than 0.");
		} else if (SIZE_CONFIG_SET.contains(option)) {
			Preconditions.checkArgument(((MemorySize) value).getBytes() > 0,
				"Configured size for key" + key + " must be larger than 0.");
		}
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
}
