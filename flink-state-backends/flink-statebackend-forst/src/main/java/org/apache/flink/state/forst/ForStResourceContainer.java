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

package org.apache.flink.state.forst;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.memory.OpaqueMemoryResource;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Filter;
import org.rocksdb.FlinkEnv;
import org.rocksdb.IndexType;
import org.rocksdb.PlainTableConfig;
import org.rocksdb.ReadOptions;
import org.rocksdb.Statistics;
import org.rocksdb.TableFormatConfig;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

/**
 * The container for ForSt resources, including option factory and shared resource among instances.
 *
 * <p>This should be the only entrance for ForStStateBackend to get ForSt options, and should be
 * properly (and necessarily) closed to prevent resource leak.
 */
public final class ForStResourceContainer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ForStResourceContainer.class);

    private static final String FORST_RELOCATE_LOG_SUFFIX = "_LOG";

    // the filename length limit is 255 on most operating systems
    private static final int INSTANCE_PATH_LENGTH_LIMIT = 255 - FORST_RELOCATE_LOG_SUFFIX.length();

    private static final String DB_DIR_STRING = "db";

    @Nullable private final URI remoteBasePath;

    @Nullable private final URI remoteForStPath;

    @Nullable private final File localBasePath;

    @Nullable private final File localForStPath;

    /** The configurations from file. */
    private final ReadableConfig configuration;

    /** The options factory to create the ForSt options. */
    @Nullable private final ForStOptionsFactory optionsFactory;

    /**
     * The shared resource among ForSt instances. This resource is not part of the 'handlesToClose',
     * because the handles to close are closed quietly, whereas for this one, we want exceptions to
     * be reported.
     */
    @Nullable private final OpaqueMemoryResource<ForStSharedResources> sharedResources;

    private final boolean enableStatistics;

    /** The handles to be closed when the container is closed. */
    private final ArrayList<AutoCloseable> handlesToClose;

    @Nullable private Path relocatedDbLogBaseDir;

    @VisibleForTesting
    public ForStResourceContainer() {
        this(new Configuration(), null, null, null, null, false);
    }

    @VisibleForTesting
    public ForStResourceContainer(@Nullable ForStOptionsFactory optionsFactory) {
        this(new Configuration(), optionsFactory, null, null, null, false);
    }

    @VisibleForTesting
    public ForStResourceContainer(
            @Nullable ForStOptionsFactory optionsFactory,
            @Nullable OpaqueMemoryResource<ForStSharedResources> sharedResources) {
        this(new Configuration(), optionsFactory, sharedResources, null, null, false);
    }

    public ForStResourceContainer(
            ReadableConfig configuration,
            @Nullable ForStOptionsFactory optionsFactory,
            @Nullable OpaqueMemoryResource<ForStSharedResources> sharedResources,
            @Nullable File localBasePath,
            @Nullable URI remoteBasePath,
            boolean enableStatistics) {

        this.configuration = configuration;
        this.optionsFactory = optionsFactory;
        this.sharedResources = sharedResources;

        this.localBasePath = localBasePath;
        this.localForStPath = localBasePath != null ? new File(localBasePath, DB_DIR_STRING) : null;
        this.remoteBasePath = remoteBasePath;
        this.remoteForStPath =
                remoteBasePath != null ? remoteBasePath.resolve(DB_DIR_STRING) : null;

        this.enableStatistics = enableStatistics;
        this.handlesToClose = new ArrayList<>();
    }

    /** Gets the ForSt {@link DBOptions} to be used for ForSt instances. */
    public DBOptions getDbOptions() {
        // initial options from common profile
        DBOptions opt = createBaseCommonDBOptions();
        handlesToClose.add(opt);

        // load configurable options on top of pre-defined profile
        setDBOptionsFromConfigurableOptions(opt);

        // add user-defined options factory, if specified
        if (optionsFactory != null) {
            opt = optionsFactory.createDBOptions(opt, handlesToClose);
        }

        // add necessary default options
        opt = opt.setCreateIfMissing(true).setAvoidFlushDuringShutdown(true);

        // if sharedResources is non-null, use the write buffer manager from it.
        if (sharedResources != null) {
            opt.setWriteBufferManager(sharedResources.getResourceHandle().getWriteBufferManager());
        }

        if (enableStatistics) {
            Statistics statistics = new Statistics();
            opt.setStatistics(statistics);
            handlesToClose.add(statistics);
        }

        // TODO: Fallback to checkpoint directory when checkpoint feature is ready if not
        // configured,
        //  fallback to local directory currently temporarily.
        if (remoteForStPath != null) {
            opt.setEnv(new FlinkEnv(remoteForStPath.toString()));
        }

        return opt;
    }

    /** Gets the ForSt {@link ColumnFamilyOptions} to be used for all ForSt instances. */
    public ColumnFamilyOptions getColumnOptions() {
        // initial options from common profile
        ColumnFamilyOptions opt = createBaseCommonColumnOptions();
        handlesToClose.add(opt);

        // load configurable options on top of pre-defined profile
        setColumnFamilyOptionsFromConfigurableOptions(opt, handlesToClose);

        // add user-defined options, if specified
        if (optionsFactory != null) {
            opt = optionsFactory.createColumnOptions(opt, handlesToClose);
        }

        // if sharedResources is non-null, use the block cache from it and
        // set necessary options for performance consideration with memory control
        if (sharedResources != null) {
            final ForStSharedResources rocksResources = sharedResources.getResourceHandle();
            final Cache blockCache = rocksResources.getCache();
            TableFormatConfig tableFormatConfig = opt.tableFormatConfig();
            BlockBasedTableConfig blockBasedTableConfig;
            if (tableFormatConfig == null) {
                blockBasedTableConfig = new BlockBasedTableConfig();
            } else {
                Preconditions.checkArgument(
                        tableFormatConfig instanceof BlockBasedTableConfig,
                        "We currently only support BlockBasedTableConfig When bounding total memory.");
                blockBasedTableConfig = (BlockBasedTableConfig) tableFormatConfig;
            }
            if (rocksResources.isUsingPartitionedIndexFilters()
                    && overwriteFilterIfExist(blockBasedTableConfig)) {
                blockBasedTableConfig.setIndexType(IndexType.kTwoLevelIndexSearch);
                blockBasedTableConfig.setPartitionFilters(true);
                blockBasedTableConfig.setPinTopLevelIndexAndFilter(true);
            }
            blockBasedTableConfig.setBlockCache(blockCache);
            blockBasedTableConfig.setCacheIndexAndFilterBlocks(true);
            blockBasedTableConfig.setCacheIndexAndFilterBlocksWithHighPriority(true);
            blockBasedTableConfig.setPinL0FilterAndIndexBlocksInCache(true);
            opt.setTableFormatConfig(blockBasedTableConfig);
        }

        return opt;
    }

    /** Gets the ForSt {@link WriteOptions} to be used for write operations. */
    public WriteOptions getWriteOptions() {
        // Disable WAL by default
        WriteOptions opt = new WriteOptions().setDisableWAL(true);
        handlesToClose.add(opt);

        // add user-defined options factory, if specified
        if (optionsFactory != null) {
            opt = optionsFactory.createWriteOptions(opt, handlesToClose);
        }

        return opt;
    }

    /** Gets the ForSt {@link ReadOptions} to be used for read operations. */
    public ReadOptions getReadOptions() {
        ReadOptions opt = new ReadOptions();
        handlesToClose.add(opt);

        // add user-defined options factory, if specified
        if (optionsFactory != null) {
            opt = optionsFactory.createReadOptions(opt, handlesToClose);
        }

        return opt;
    }

    @Nullable
    public File getLocalBasePath() {
        return localBasePath;
    }

    @Nullable
    public File getLocalForStPath() {
        return localForStPath;
    }

    @Nullable
    public URI getRemoteBasePath() {
        return remoteBasePath;
    }

    /**
     * Prepare local and remote directories.
     *
     * @throws Exception if any unexpected behaviors.
     */
    public void prepareDirectories() throws Exception {
        if (remoteBasePath != null && remoteForStPath != null) {
            prepareDirectories(remoteBasePath, remoteForStPath);
        }
        if (localBasePath != null && localForStPath != null) {
            prepareDirectories(new URI(localBasePath.getPath()), new URI(localForStPath.getPath()));
        }
    }

    private static void prepareDirectories(URI basePath, URI dbPath) throws IOException {
        FileSystem fileSystem = FileSystem.get(basePath);
        org.apache.flink.core.fs.Path tempBasePath = new org.apache.flink.core.fs.Path(basePath),
                tempDBPath = new org.apache.flink.core.fs.Path(dbPath);
        if (fileSystem.exists(tempBasePath)) {
            if (!fileSystem.getFileStatus(tempBasePath).isDir()) {
                throw new IOException("Not a directory: " + tempBasePath);
            }
        } else if (!fileSystem.mkdirs(tempBasePath)) {
            throw new IOException(
                    String.format("Could not create ForSt directory at %s.", tempBasePath));
        }
        if (fileSystem.exists(tempDBPath)) {
            fileSystem.delete(tempDBPath, true);
        }
        if (!fileSystem.mkdirs(tempDBPath)) {
            throw new IOException(
                    String.format("Could not create ForSt db directory at %s.", tempDBPath));
        }
    }

    /**
     * Clear local and remote directories.
     *
     * @throws Exception if any unexpected behaviors.
     */
    public void clearDirectories() throws Exception {
        if (remoteBasePath != null) {
            clearDirectories(remoteBasePath);
        }
        if (localBasePath != null) {
            clearDirectories(new URI(localBasePath.getPath()));
        }
    }

    private static void clearDirectories(URI basePath) throws IOException {
        FileSystem fileSystem = FileSystem.get(basePath);
        org.apache.flink.core.fs.Path tempBasePath = new org.apache.flink.core.fs.Path(basePath);
        if (fileSystem.exists(tempBasePath)) {
            fileSystem.delete(tempBasePath, true);
        }
    }

    ForStNativeMetricOptions getMemoryWatcherOptions(
            ForStNativeMetricOptions defaultMetricOptions) {
        return optionsFactory == null
                ? defaultMetricOptions
                : optionsFactory.createNativeMetricsOptions(defaultMetricOptions);
    }

    @Override
    public void close() throws Exception {
        handlesToClose.forEach(IOUtils::closeQuietly);
        handlesToClose.clear();

        if (sharedResources != null) {
            sharedResources.close();
        }
        cleanRelocatedDbLogs();
    }

    /**
     * Overwrite configured {@link Filter} if enable partitioned filter. Partitioned filter only
     * worked in full bloom filter, not blocked based.
     */
    private boolean overwriteFilterIfExist(BlockBasedTableConfig blockBasedTableConfig) {
        if (blockBasedTableConfig.filterPolicy() != null) {
            // TODO Can get filter's config in the future ForSt version, and build new filter use
            // existing config.
            BloomFilter newFilter = new BloomFilter(10, false);
            LOG.info(
                    "Existing filter has been overwritten to full filters since partitioned index filters is enabled.");
            blockBasedTableConfig.setFilterPolicy(newFilter);
            handlesToClose.add(newFilter);
        }
        return true;
    }

    /** Create a {@link DBOptions} for ForSt, including some common settings. */
    DBOptions createBaseCommonDBOptions() {
        return new DBOptions().setUseFsync(false).setStatsDumpPeriodSec(0);
    }

    /** Create a {@link ColumnFamilyOptions} for ForSt, including some common settings. */
    ColumnFamilyOptions createBaseCommonColumnOptions() {
        return new ColumnFamilyOptions();
    }

    /**
     * Get a value for option from pre-defined option and configurable option settings. The priority
     * relationship is as below.
     *
     * <p>Configured value > pre-defined value > default value.
     *
     * @param option the wanted option
     * @param <T> the value type
     * @return the final value for the option according to the priority above.
     */
    @Nullable
    private <T> T internalGetOption(ConfigOption<T> option) {
        return configuration.getOptional(option).orElseGet(option::defaultValue);
    }

    @SuppressWarnings("ConstantConditions")
    private DBOptions setDBOptionsFromConfigurableOptions(DBOptions currentOptions) {

        currentOptions.setMaxBackgroundJobs(
                internalGetOption(ForStConfigurableOptions.MAX_BACKGROUND_THREADS));

        currentOptions.setMaxOpenFiles(internalGetOption(ForStConfigurableOptions.MAX_OPEN_FILES));

        currentOptions.setInfoLogLevel(internalGetOption(ForStConfigurableOptions.LOG_LEVEL));

        String logDir = internalGetOption(ForStConfigurableOptions.LOG_DIR);
        if (logDir == null || logDir.isEmpty()) {
            if (localForStPath == null
                    || localForStPath.getAbsolutePath().length() <= INSTANCE_PATH_LENGTH_LIMIT) {
                relocateDefaultDbLogDir(currentOptions);
            } else {
                // disable log relocate when instance path length exceeds limit to prevent ForSt
                // log file creation failure, details in FLINK-31743
                LOG.warn(
                        "ForSt local path length exceeds limit : {}, disable log relocate.",
                        localForStPath);
            }
        } else {
            currentOptions.setDbLogDir(logDir);
        }

        currentOptions.setMaxLogFileSize(
                internalGetOption(ForStConfigurableOptions.LOG_MAX_FILE_SIZE).getBytes());

        currentOptions.setKeepLogFileNum(internalGetOption(ForStConfigurableOptions.LOG_FILE_NUM));

        return currentOptions;
    }

    @SuppressWarnings("ConstantConditions")
    private ColumnFamilyOptions setColumnFamilyOptionsFromConfigurableOptions(
            ColumnFamilyOptions currentOptions, Collection<AutoCloseable> handlesToClose) {

        currentOptions.setCompactionStyle(
                internalGetOption(ForStConfigurableOptions.COMPACTION_STYLE));

        currentOptions.setCompressionPerLevel(
                internalGetOption(ForStConfigurableOptions.COMPRESSION_PER_LEVEL));

        currentOptions.setLevelCompactionDynamicLevelBytes(
                internalGetOption(ForStConfigurableOptions.USE_DYNAMIC_LEVEL_SIZE));

        currentOptions.setTargetFileSizeBase(
                internalGetOption(ForStConfigurableOptions.TARGET_FILE_SIZE_BASE).getBytes());

        currentOptions.setMaxBytesForLevelBase(
                internalGetOption(ForStConfigurableOptions.MAX_SIZE_LEVEL_BASE).getBytes());

        currentOptions.setWriteBufferSize(
                internalGetOption(ForStConfigurableOptions.WRITE_BUFFER_SIZE).getBytes());

        currentOptions.setMaxWriteBufferNumber(
                internalGetOption(ForStConfigurableOptions.MAX_WRITE_BUFFER_NUMBER));

        currentOptions.setMinWriteBufferNumberToMerge(
                internalGetOption(ForStConfigurableOptions.MIN_WRITE_BUFFER_NUMBER_TO_MERGE));

        TableFormatConfig tableFormatConfig = currentOptions.tableFormatConfig();

        BlockBasedTableConfig blockBasedTableConfig;
        if (tableFormatConfig == null) {
            blockBasedTableConfig = new BlockBasedTableConfig();
        } else {
            if (tableFormatConfig instanceof PlainTableConfig) {
                // if the table format config is PlainTableConfig, we just return current
                // column-family options
                return currentOptions;
            } else {
                blockBasedTableConfig = (BlockBasedTableConfig) tableFormatConfig;
            }
        }

        blockBasedTableConfig.setBlockSize(
                internalGetOption(ForStConfigurableOptions.BLOCK_SIZE).getBytes());

        blockBasedTableConfig.setMetadataBlockSize(
                internalGetOption(ForStConfigurableOptions.METADATA_BLOCK_SIZE).getBytes());

        blockBasedTableConfig.setBlockCacheSize(
                internalGetOption(ForStConfigurableOptions.BLOCK_CACHE_SIZE).getBytes());

        if (internalGetOption(ForStConfigurableOptions.USE_BLOOM_FILTER)) {
            final double bitsPerKey =
                    internalGetOption(ForStConfigurableOptions.BLOOM_FILTER_BITS_PER_KEY);
            final boolean blockBasedMode =
                    internalGetOption(ForStConfigurableOptions.BLOOM_FILTER_BLOCK_BASED_MODE);
            BloomFilter bloomFilter = new BloomFilter(bitsPerKey, blockBasedMode);
            handlesToClose.add(bloomFilter);
            blockBasedTableConfig.setFilterPolicy(bloomFilter);
        }

        return currentOptions.setTableFormatConfig(blockBasedTableConfig);
    }

    /**
     * Relocates the default log directory of ForSt with the Flink log directory. Finds the Flink
     * log directory using log.file Java property that is set during startup.
     *
     * @param dbOptions The ForSt {@link DBOptions}.
     */
    private void relocateDefaultDbLogDir(DBOptions dbOptions) {
        String logFilePath = System.getProperty("log.file");
        if (logFilePath != null) {
            File logFile = resolveFileLocation(logFilePath);
            if (logFile != null && resolveFileLocation(logFile.getParent()) != null) {
                String relocatedDbLogDir = logFile.getParent();
                this.relocatedDbLogBaseDir = new File(relocatedDbLogDir).toPath();
                dbOptions.setDbLogDir(relocatedDbLogDir);
            }
        }
    }

    /**
     * Verify log file location.
     *
     * @param logFilePath Path to log file
     * @return File or null if not a valid log file
     */
    private File resolveFileLocation(String logFilePath) {
        File logFile = new File(logFilePath);
        return (logFile.exists() && logFile.canRead()) ? logFile : null;
    }

    /** Clean all relocated ForSt logs. */
    private void cleanRelocatedDbLogs() {
        if (localForStPath != null && relocatedDbLogBaseDir != null) {
            LOG.info("Cleaning up relocated ForSt logs: {}.", relocatedDbLogBaseDir);

            String relocatedDbLogPrefix =
                    resolveRelocatedDbLogPrefix(localForStPath.getAbsolutePath());
            try {
                Arrays.stream(FileUtils.listDirectory(relocatedDbLogBaseDir))
                        .filter(
                                path ->
                                        !Files.isDirectory(path)
                                                && path.toFile()
                                                        .getName()
                                                        .startsWith(relocatedDbLogPrefix))
                        .forEach(IOUtils::deleteFileQuietly);
            } catch (IOException e) {
                LOG.warn("Could not list relocated ForSt log directory: {}", relocatedDbLogBaseDir);
            }
        }
    }

    /**
     * Resolve the prefix of ForSt's log file name according to ForSt's log file name rules.
     *
     * @param instanceForStAbsolutePath The path where the ForSt directory is located.
     * @return Resolved ForSt log name prefix.
     */
    private String resolveRelocatedDbLogPrefix(String instanceForStAbsolutePath) {
        if (!instanceForStAbsolutePath.isEmpty()
                && !instanceForStAbsolutePath.matches("^[a-zA-Z0-9\\-._].*")) {
            instanceForStAbsolutePath = instanceForStAbsolutePath.substring(1);
        }
        return instanceForStAbsolutePath.replaceAll("[^a-zA-Z0-9\\-._]", "_")
                + FORST_RELOCATE_LOG_SUFFIX;
    }
}
