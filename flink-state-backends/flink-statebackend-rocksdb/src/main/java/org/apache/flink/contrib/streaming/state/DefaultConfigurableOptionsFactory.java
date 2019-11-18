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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.contrib.streaming.state.RocksDBConfigurableOptions.BLOCK_CACHE_SIZE;
import static org.apache.flink.contrib.streaming.state.RocksDBConfigurableOptions.BLOCK_CACHE_SIZE_MEMORYSIZE;
import static org.apache.flink.contrib.streaming.state.RocksDBConfigurableOptions.BLOCK_SIZE;
import static org.apache.flink.contrib.streaming.state.RocksDBConfigurableOptions.BLOCK_SIZE_MEMORYSIZE;
import static org.apache.flink.contrib.streaming.state.RocksDBConfigurableOptions.COMPACTION_STYLE;
import static org.apache.flink.contrib.streaming.state.RocksDBConfigurableOptions.MAX_BACKGROUND_THREADS;
import static org.apache.flink.contrib.streaming.state.RocksDBConfigurableOptions.MAX_OPEN_FILES;
import static org.apache.flink.contrib.streaming.state.RocksDBConfigurableOptions.MAX_SIZE_LEVEL_BASE;
import static org.apache.flink.contrib.streaming.state.RocksDBConfigurableOptions.MAX_SIZE_LEVEL_BASE_MEMORYSIZE;
import static org.apache.flink.contrib.streaming.state.RocksDBConfigurableOptions.MAX_WRITE_BUFFER_NUMBER;
import static org.apache.flink.contrib.streaming.state.RocksDBConfigurableOptions.MIN_WRITE_BUFFER_NUMBER_TO_MERGE;
import static org.apache.flink.contrib.streaming.state.RocksDBConfigurableOptions.TARGET_FILE_SIZE_BASE;
import static org.apache.flink.contrib.streaming.state.RocksDBConfigurableOptions.TARGET_FILE_SIZE_BASE_MEMORYSIZE;
import static org.apache.flink.contrib.streaming.state.RocksDBConfigurableOptions.USE_DYNAMIC_LEVEL_SIZE;
import static org.apache.flink.contrib.streaming.state.RocksDBConfigurableOptions.WRITE_BUFFER_SIZE;
import static org.apache.flink.contrib.streaming.state.RocksDBConfigurableOptions.WRITE_BUFFER_SIZE_MEMORYSIZE;

/**
 * An implementation of {@link ConfigurableOptionsFactory} using options provided by {@link RocksDBConfigurableOptions}
 * and acted as the default options factory within {@link RocksDBStateBackend} if user not defined a {@link OptionsFactory}.
 *
 * <p>This implementation also provide some setters to let user could create a {@link OptionsFactory} conveniently。
 */
public class DefaultConfigurableOptionsFactory implements ConfigurableOptionsFactory {

	private final Map<String, String> configuredOptions = new HashMap<>();
	private final Map<String, MemorySize> sizeConfigMap = new HashMap<>();
	private static final Map<String, String> SIZE_CONFIG_KEY_MAPPING = new HashMap<String, String>() {{
		put(TARGET_FILE_SIZE_BASE.key(), TARGET_FILE_SIZE_BASE_MEMORYSIZE.key());
		put(MAX_SIZE_LEVEL_BASE.key(), MAX_SIZE_LEVEL_BASE_MEMORYSIZE.key());
		put(WRITE_BUFFER_SIZE.key(), WRITE_BUFFER_SIZE_MEMORYSIZE.key());
		put(BLOCK_SIZE.key(), BLOCK_SIZE_MEMORYSIZE.key());
		put(BLOCK_CACHE_SIZE.key(), BLOCK_CACHE_SIZE_MEMORYSIZE.key());
	}};

	@Override
	public DBOptions createDBOptions(DBOptions currentOptions) {
		if (isOptionConfigured(MAX_BACKGROUND_THREADS)) {
			currentOptions.setIncreaseParallelism(getMaxBackgroundThreads());
		}

		if (isOptionConfigured(MAX_OPEN_FILES)) {
			currentOptions.setMaxOpenFiles(getMaxOpenFiles());
		}

		return currentOptions;
	}

	@Override
	public ColumnFamilyOptions createColumnOptions(ColumnFamilyOptions currentOptions) {
		if (isOptionConfigured(COMPACTION_STYLE)) {
			currentOptions.setCompactionStyle(getCompactionStyle());
		}

		if (isOptionConfigured(USE_DYNAMIC_LEVEL_SIZE)) {
			currentOptions.setLevelCompactionDynamicLevelBytes(getUseDynamicLevelSize());
		}

		if (isOptionConfigured(TARGET_FILE_SIZE_BASE_MEMORYSIZE)) {
			currentOptions.setTargetFileSizeBase(getTargetFileSizeBase());
		}

		if (isOptionConfigured(MAX_SIZE_LEVEL_BASE_MEMORYSIZE)) {
			currentOptions.setMaxBytesForLevelBase(getMaxSizeLevelBase());
		}

		if (isOptionConfigured(WRITE_BUFFER_SIZE_MEMORYSIZE)) {
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

		if (isOptionConfigured(BLOCK_SIZE_MEMORYSIZE)) {
			blockBasedTableConfig.setBlockSize(getBlockSize());
		}

		if (isOptionConfigured(BLOCK_CACHE_SIZE_MEMORYSIZE)) {
			blockBasedTableConfig.setBlockCacheSize(getBlockCacheSize());
		}

		return currentOptions.setTableFormatConfig(blockBasedTableConfig);
	}

	public Map<String, String> getConfiguredOptions() {
		return new HashMap<>(configuredOptions);
	}

	private boolean isOptionConfigured(ConfigOption configOption) {
		return configuredOptions.containsKey(configOption.key()) || sizeConfigMap.containsKey(configOption.key());
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
		return sizeConfigMap.get(TARGET_FILE_SIZE_BASE_MEMORYSIZE.key()).getBytes();
	}

	@Deprecated
	public DefaultConfigurableOptionsFactory setTargetFileSizeBase(String targetFileSizeBase) {
		MemorySize memorySize = MemorySize.parse(targetFileSizeBase);
		Preconditions.checkArgument(memorySize.getBytes() > 0,
			"Invalid configuration " + targetFileSizeBase + " for target file size base.");
		return setTargetFileSizeBase(memorySize);
	}

	public DefaultConfigurableOptionsFactory setTargetFileSizeBase(MemorySize targetFileSizeBase) {
		Preconditions.checkArgument(targetFileSizeBase.getBytes() > 0,
			"Invalid configuration " + targetFileSizeBase + " for target file size base.");
		setMemorySizeInternal(TARGET_FILE_SIZE_BASE_MEMORYSIZE.key(), targetFileSizeBase);
		return this;
	}

	//--------------------------------------------------------------------------
	// Maximum total data size for a level, i.e., the max total size for level-1
	//--------------------------------------------------------------------------

	private long getMaxSizeLevelBase() {
		return sizeConfigMap.get(MAX_SIZE_LEVEL_BASE_MEMORYSIZE.key()).getBytes();
	}

	@Deprecated
	public DefaultConfigurableOptionsFactory setMaxSizeLevelBase(String maxSizeLevelBase) {
		MemorySize memorySize = MemorySize.parse(maxSizeLevelBase);
		Preconditions.checkArgument(memorySize.getBytes() > 0,
			"Invalid configuration " + maxSizeLevelBase + " for max size of level base.");
		return setMaxSizeLevelBase(memorySize);
	}

	public DefaultConfigurableOptionsFactory setMaxSizeLevelBase(MemorySize maxSizeLevelBase) {
		Preconditions.checkArgument(maxSizeLevelBase != null && maxSizeLevelBase.getBytes() > 0,
			"Invalid configuration " + maxSizeLevelBase + " for max size of level base.");
		setMemorySizeInternal(MAX_SIZE_LEVEL_BASE_MEMORYSIZE.key(), maxSizeLevelBase);
		return this;
	}

	//--------------------------------------------------------------------------
	// Amount of data to build up in memory (backed by an unsorted log on disk)
	// before converting to a sorted on-disk file. Larger values increase
	// performance, especially during bulk loads.
	//--------------------------------------------------------------------------

	private long getWriteBufferSize() {
		return sizeConfigMap.get(WRITE_BUFFER_SIZE_MEMORYSIZE.key()).getBytes();
	}

	@Deprecated
	public DefaultConfigurableOptionsFactory setWriteBufferSize(String writeBufferSize) {
		MemorySize memorySize = MemorySize.parse(writeBufferSize);
		Preconditions.checkArgument(memorySize.getBytes() > 0,
			"Invalid configuration " + writeBufferSize + " for write-buffer size.");

		return setWriteBufferSize(memorySize);
	}

	public DefaultConfigurableOptionsFactory setWriteBufferSize(MemorySize writeBufferSize) {
		Preconditions.checkArgument(writeBufferSize != null && writeBufferSize.getBytes() > 0,
			"Invalid configuration " + writeBufferSize + " frr write-buffer size.");
		setMemorySizeInternal(WRITE_BUFFER_SIZE_MEMORYSIZE.key(), writeBufferSize);
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
		return sizeConfigMap.get(BLOCK_SIZE_MEMORYSIZE.key()).getBytes();
	}

	@Deprecated
	public DefaultConfigurableOptionsFactory setBlockSize(String blockSize) {
		MemorySize memorySize = MemorySize.parse(blockSize);
		Preconditions.checkArgument(memorySize.getBytes() > 0,
			"Invalid configuration " + blockSize + " for block size.");
		return setBlockSize(memorySize);
	}

	public DefaultConfigurableOptionsFactory setBlockSize(MemorySize blockSize) {
		Preconditions.checkArgument(blockSize != null && blockSize.getBytes() > 0, "Invalid configuration " + blockSize + " for block size");
		setMemorySizeInternal(BLOCK_SIZE_MEMORYSIZE.key(), blockSize);
		return this;
	}

	//--------------------------------------------------------------------------
	// The amount of the cache for data blocks in RocksDB
	//--------------------------------------------------------------------------

	private long getBlockCacheSize() {
		return sizeConfigMap.get(BLOCK_CACHE_SIZE_MEMORYSIZE.key()).getBytes();
	}

	@Deprecated
	public DefaultConfigurableOptionsFactory setBlockCacheSize(String blockCacheSize) {
		MemorySize memorySize = MemorySize.parse(blockCacheSize);
		Preconditions.checkArgument(memorySize.getBytes() > 0,
			"Invalid configuration " + blockCacheSize + " for block cache size.");
		return setBlockCacheSize(memorySize);
	}

	public DefaultConfigurableOptionsFactory setBlockCacheSize(MemorySize blockCacheSize) {
		Preconditions.checkArgument(blockCacheSize != null && blockCacheSize.getBytes() > 0, "Invalid configuration " + blockCacheSize + " fo  block cache seee.");
		setMemorySizeInternal(BLOCK_CACHE_SIZE_MEMORYSIZE.key(), blockCacheSize);

		return this;
	}

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

		TARGET_FILE_SIZE_BASE_MEMORYSIZE.key(),
		MAX_SIZE_LEVEL_BASE_MEMORYSIZE.key(),
		WRITE_BUFFER_SIZE_MEMORYSIZE.key(),
		BLOCK_SIZE_MEMORYSIZE.key(),
		BLOCK_CACHE_SIZE_MEMORYSIZE.key()
	};

	private static final Set<String> POSITIVE_INT_CONFIG_SET = new HashSet<>(Arrays.asList(
		MAX_BACKGROUND_THREADS.key(),
		MAX_WRITE_BUFFER_NUMBER.key(),
		MIN_WRITE_BUFFER_NUMBER_TO_MERGE.key()
	));

	private static final Set<String> DEPRECATED_SIZE_CONFIG_SET = new HashSet<>(Arrays.asList(
		TARGET_FILE_SIZE_BASE.key(),
		MAX_SIZE_LEVEL_BASE.key(),
		WRITE_BUFFER_SIZE.key(),
		BLOCK_SIZE.key(),
		BLOCK_CACHE_SIZE.key()
	));

	private static final Map<String, ConfigOption<MemorySize>> SIZE_CONFIG_SET = new HashMap<String, ConfigOption<MemorySize>>() {{
		put(TARGET_FILE_SIZE_BASE_MEMORYSIZE.key(), TARGET_FILE_SIZE_BASE_MEMORYSIZE);
		put(MAX_SIZE_LEVEL_BASE_MEMORYSIZE.key(), MAX_SIZE_LEVEL_BASE_MEMORYSIZE);
		put(WRITE_BUFFER_SIZE_MEMORYSIZE.key(), WRITE_BUFFER_SIZE_MEMORYSIZE);
		put(BLOCK_SIZE_MEMORYSIZE.key(), BLOCK_SIZE_MEMORYSIZE);
		put(BLOCK_CACHE_SIZE_MEMORYSIZE.key(), BLOCK_CACHE_SIZE_MEMORYSIZE);
	}};

	private static final Set<String> BOOLEAN_CONFIG_SET = new HashSet<>(Collections.singletonList(
		USE_DYNAMIC_LEVEL_SIZE.key()
	));

	private static final Set<String> COMPACTION_STYLE_SET = Arrays.stream(CompactionStyle.values())
		.map(c -> c.name().toLowerCase()).collect(Collectors.toSet());

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
		preCheckSizeConfigKey(configuration);
		for (String key : CANDIDATE_CONFIGS) {
			if (SIZE_CONFIG_SET.containsKey(key)) {
				if (configuration.containsKey(key)) {
					MemorySize memorySize = configuration.get(SIZE_CONFIG_SET.get(key));
					Preconditions.checkArgument(memorySize.getBytes() > 0, "Configuration for " + key + " must be positive.");
					this.sizeConfigMap.put(key, memorySize);
				}
			} else {
				String newValue = configuration.getString(key, null);

				if (newValue != null) {
					if (checkArgumentValid(key, newValue)) {
						if (DEPRECATED_SIZE_CONFIG_SET.contains(key)) {
							this.sizeConfigMap.put(SIZE_CONFIG_KEY_MAPPING.get(key), MemorySize.parse(newValue));
						} else {
							this.configuredOptions.put(key, newValue);
						}
					}
				}
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
	 * @param key The configuration key which is configurable in {@link RocksDBConfigurableOptions}.
	 * @param value The value within given configuration.
	 *
	 * @return whether the given key and value in string format is legal.
	 */
	private static boolean checkArgumentValid(String key, String value) {
		if (POSITIVE_INT_CONFIG_SET.contains(key)) {

			Preconditions.checkArgument(Integer.parseInt(value) > 0,
				"Configured value for key: " + key + " must be larger than 0.");
		} else if (DEPRECATED_SIZE_CONFIG_SET.contains(key)) {

			Preconditions.checkArgument(MemorySize.parseBytes(value) > 0,
				"Configured size for key" + key + " must be larger than 0.");
		} else if (BOOLEAN_CONFIG_SET.contains(key)) {

			Preconditions.checkArgument("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value),
				"The configured boolean value: " + value + " for key: " + key + " is illegal.");
		} else if (key.equals(COMPACTION_STYLE.key())) {
			value = value.toLowerCase();
			Preconditions.checkArgument(COMPACTION_STYLE_SET.contains(value),
				"Compression type: " + value + " is not recognized with legal types: " + String.join(", ", COMPACTION_STYLE_SET));
		}
		return true;
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

	private void setMemorySizeInternal(String key, MemorySize value) {
		Preconditions.checkArgument(value != null && value.getBytes() > 0, "The configuration value must not be null and be positive.");
		sizeConfigMap.put(key, value);
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
	 * Check that whether there configured both the size key and deprecated size key, throw IllegalArgumentException if set both.
	 * @param config Which used to check
	 */
	private void preCheckSizeConfigKey(Configuration config) {
		for (Map.Entry<String, String> entry : SIZE_CONFIG_KEY_MAPPING.entrySet()) {
			Preconditions.checkArgument(!(config.containsKey(entry.getKey()) && config.containsKey(entry.getValue())), "Can not set both key " + entry.getValue() + " add deprecated key " + entry.getKey());
		}
	}
}
