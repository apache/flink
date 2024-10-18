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

package org.apache.flink.state.rocksdb;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.description.InlineElement;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.memory.OpaqueMemoryResource;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractManagedMemoryStateBackend;
import org.apache.flink.runtime.state.ConfigurableStateBackend;
import org.apache.flink.runtime.state.DefaultOperatorStateBackendBuilder;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.state.rocksdb.RocksDBMemoryControllerUtils.RocksDBMemoryFactory;
import org.apache.flink.state.rocksdb.sstmerge.RocksDBManualCompactionConfig;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.DynamicCodeLoadingException;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.MdcUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TernaryBoolean;

import org.rocksdb.NativeLibraryLoader;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.function.Supplier;

import static org.apache.flink.configuration.description.TextElement.text;
import static org.apache.flink.state.rocksdb.RocksDBConfigurableOptions.INCREMENTAL_RESTORE_ASYNC_COMPACT_AFTER_RESCALE;
import static org.apache.flink.state.rocksdb.RocksDBConfigurableOptions.RESTORE_OVERLAP_FRACTION_THRESHOLD;
import static org.apache.flink.state.rocksdb.RocksDBConfigurableOptions.USE_DELETE_FILES_IN_RANGE_DURING_RESCALING;
import static org.apache.flink.state.rocksdb.RocksDBConfigurableOptions.USE_INGEST_DB_RESTORE_MODE;
import static org.apache.flink.state.rocksdb.RocksDBConfigurableOptions.WRITE_BATCH_SIZE;
import static org.apache.flink.state.rocksdb.RocksDBOptions.CHECKPOINT_TRANSFER_THREAD_NUM;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link org.apache.flink.runtime.state.StateBackend} that stores its state in an embedded {@code
 * RocksDB} instance. This state backend can store very large state that exceeds memory and spills
 * to local disk. All key/value state (including windows) is stored in the key/value index of
 * RocksDB. For persistence against loss of machines, please configure a {@link
 * org.apache.flink.runtime.state.CheckpointStorage} instance for the Job.
 *
 * <p>The behavior of the RocksDB instances can be parametrized by setting RocksDB Options using the
 * methods {@link #setPredefinedOptions(PredefinedOptions)} and {@link
 * #setRocksDBOptions(RocksDBOptionsFactory)}.
 */
@PublicEvolving
public class EmbeddedRocksDBStateBackend extends AbstractManagedMemoryStateBackend
        implements ConfigurableStateBackend {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(EmbeddedRocksDBStateBackend.class);

    /** The number of (re)tries for loading the RocksDB JNI library. */
    private static final int ROCKSDB_LIB_LOADING_ATTEMPTS = 3;

    /** Flag whether the native library has been loaded. */
    private static boolean rocksDbInitialized = false;

    private static final int UNDEFINED_NUMBER_OF_TRANSFER_THREADS = -1;

    private static final long UNDEFINED_WRITE_BATCH_SIZE = -1;

    private static final double UNDEFINED_OVERLAP_FRACTION_THRESHOLD = -1;

    // ------------------------------------------------------------------------

    // -- configuration values, set in the application / configuration

    /**
     * Base paths for RocksDB directory, as configured. Null if not yet set, in which case the
     * configuration values will be used. The configuration defaults to the TaskManager's temp
     * directories.
     */
    @Nullable private File[] localRocksDbDirectories;

    /** The pre-configured option settings. */
    @Nullable private PredefinedOptions predefinedOptions;

    /** The configurable options. */
    @Nullable private ReadableConfig configurableOptions;

    /** The options factory to create the RocksDB options in the cluster. */
    @Nullable private RocksDBOptionsFactory rocksDbOptionsFactory;

    /** This determines if incremental checkpointing is enabled. */
    private final TernaryBoolean enableIncrementalCheckpointing;

    /** Thread number used to transfer (download and upload) state, default value: 1. */
    private int numberOfTransferThreads;

    /** The configuration for memory settings (pool sizes, etc.). */
    private final RocksDBMemoryConfiguration memoryConfiguration;

    /**
     * The configuration for rocksdb priorityQueue state settings (priorityQueue state type, etc.).
     */
    private final RocksDBPriorityQueueConfig priorityQueueConfig;

    /** The default rocksdb property-based metrics options. */
    private final RocksDBNativeMetricOptions nativeMetricOptions;

    // -- runtime values, set on TaskManager when initializing / using the backend

    /** Base paths for RocksDB directory, as initialized. */
    private transient File[] initializedDbBasePaths;

    /** JobID for uniquifying backup paths. */
    private transient JobID jobId;

    /** The index of the next directory to be used from {@link #initializedDbBasePaths}. */
    private transient int nextDirectory;

    /** Whether we already lazily initialized our local storage directories. */
    private transient boolean isInitialized;

    /**
     * Max consumed memory size for one batch in {@link RocksDBWriteBatchWrapper}, default value
     * 2mb.
     */
    private long writeBatchSize;

    /**
     * The threshold of the overlap fraction between the handle's key-group range and target
     * key-group range.
     */
    private final double overlapFractionThreshold;

    /**
     * Whether we use the optimized Ingest/Clip DB method for rescaling RocksDB incremental
     * checkpoints.
     */
    private final TernaryBoolean useIngestDbRestoreMode;

    /**
     * Whether we trigger an async compaction after restores for which we detect state in the
     * database (including tombstones) that exceed the proclaimed key-groups range of the backend.
     */
    private final TernaryBoolean incrementalRestoreAsyncCompactAfterRescale;

    /**
     * Whether to leverage deleteFilesInRange API to clean up useless rocksdb files during
     * rescaling.
     */
    private final TernaryBoolean rescalingUseDeleteFilesInRange;

    /** Factory for Write Buffer Manager and Block Cache. */
    private RocksDBMemoryFactory rocksDBMemoryFactory;
    // ------------------------------------------------------------------------

    private final RocksDBManualCompactionConfig manualCompactionConfig;

    /** Creates a new {@code EmbeddedRocksDBStateBackend} for storing local state. */
    public EmbeddedRocksDBStateBackend() {
        this(TernaryBoolean.UNDEFINED);
    }

    /**
     * Creates a new {@code EmbeddedRocksDBStateBackend} for storing local state.
     *
     * @param enableIncrementalCheckpointing True if incremental checkpointing is enabled.
     */
    public EmbeddedRocksDBStateBackend(boolean enableIncrementalCheckpointing) {
        this(TernaryBoolean.fromBoolean(enableIncrementalCheckpointing));
    }

    /**
     * Creates a new {@code EmbeddedRocksDBStateBackend} for storing local state.
     *
     * @param enableIncrementalCheckpointing True if incremental checkpointing is enabled.
     */
    public EmbeddedRocksDBStateBackend(TernaryBoolean enableIncrementalCheckpointing) {
        this.enableIncrementalCheckpointing = enableIncrementalCheckpointing;
        this.numberOfTransferThreads = UNDEFINED_NUMBER_OF_TRANSFER_THREADS;
        this.nativeMetricOptions = new RocksDBNativeMetricOptions();
        this.memoryConfiguration = new RocksDBMemoryConfiguration();
        this.writeBatchSize = UNDEFINED_WRITE_BATCH_SIZE;
        this.overlapFractionThreshold = UNDEFINED_OVERLAP_FRACTION_THRESHOLD;
        this.rocksDBMemoryFactory = RocksDBMemoryFactory.DEFAULT;
        this.priorityQueueConfig = new RocksDBPriorityQueueConfig();
        this.useIngestDbRestoreMode = TernaryBoolean.UNDEFINED;
        this.incrementalRestoreAsyncCompactAfterRescale = TernaryBoolean.UNDEFINED;
        this.rescalingUseDeleteFilesInRange = TernaryBoolean.UNDEFINED;
        this.manualCompactionConfig = null;
    }

    /**
     * Private constructor that creates a re-configured copy of the state backend.
     *
     * @param original The state backend to re-configure.
     * @param config The configuration.
     * @param classLoader The class loader.
     */
    protected EmbeddedRocksDBStateBackend(
            EmbeddedRocksDBStateBackend original, ReadableConfig config, ClassLoader classLoader) {
        // configure incremental checkpoints
        this.enableIncrementalCheckpointing =
                original.enableIncrementalCheckpointing.resolveUndefined(
                        config.get(CheckpointingOptions.INCREMENTAL_CHECKPOINTS));

        if (original.numberOfTransferThreads == UNDEFINED_NUMBER_OF_TRANSFER_THREADS) {
            this.numberOfTransferThreads = config.get(CHECKPOINT_TRANSFER_THREAD_NUM);
        } else {
            this.numberOfTransferThreads = original.numberOfTransferThreads;
        }

        if (original.writeBatchSize == UNDEFINED_WRITE_BATCH_SIZE) {
            this.writeBatchSize = config.get(WRITE_BATCH_SIZE).getBytes();
        } else {
            this.writeBatchSize = original.writeBatchSize;
        }

        this.memoryConfiguration =
                RocksDBMemoryConfiguration.fromOtherAndConfiguration(
                        original.memoryConfiguration, config);
        this.memoryConfiguration.validate();

        this.priorityQueueConfig =
                RocksDBPriorityQueueConfig.fromOtherAndConfiguration(
                        original.priorityQueueConfig, config);

        // configure local directories
        if (original.localRocksDbDirectories != null) {
            this.localRocksDbDirectories = original.localRocksDbDirectories;
        } else {
            final String rocksdbLocalPaths = config.get(RocksDBOptions.LOCAL_DIRECTORIES);
            if (rocksdbLocalPaths != null) {
                String[] directories = rocksdbLocalPaths.split(",|" + File.pathSeparator);

                try {
                    setDbStoragePaths(directories);
                } catch (IllegalArgumentException e) {
                    throw new IllegalConfigurationException(
                            "Invalid configuration for RocksDB state "
                                    + "backend's local storage directories: "
                                    + e.getMessage(),
                            e);
                }
            }
        }

        // configure metric options
        this.nativeMetricOptions = RocksDBNativeMetricOptions.fromConfig(config);

        // configure RocksDB predefined options
        this.predefinedOptions =
                original.predefinedOptions == null
                        ? PredefinedOptions.valueOf(config.get(RocksDBOptions.PREDEFINED_OPTIONS))
                        : original.predefinedOptions;
        LOG.info("Using predefined options: {}.", predefinedOptions.name());

        // configurable options
        this.configurableOptions = mergeConfigurableOptions(original.configurableOptions, config);

        // configure RocksDB options factory
        try {
            rocksDbOptionsFactory =
                    configureOptionsFactory(
                            original.rocksDbOptionsFactory,
                            config.get(RocksDBOptions.OPTIONS_FACTORY),
                            config,
                            classLoader);
        } catch (DynamicCodeLoadingException e) {
            throw new FlinkRuntimeException(e);
        }

        // configure latency tracking
        latencyTrackingConfigBuilder = original.latencyTrackingConfigBuilder.configure(config);

        // configure overlap fraction threshold
        overlapFractionThreshold =
                original.overlapFractionThreshold == UNDEFINED_OVERLAP_FRACTION_THRESHOLD
                        ? config.get(RESTORE_OVERLAP_FRACTION_THRESHOLD)
                        : original.overlapFractionThreshold;
        checkArgument(
                overlapFractionThreshold >= 0 && this.overlapFractionThreshold <= 1,
                "Overlap fraction threshold of restoring should be between 0 and 1");

        incrementalRestoreAsyncCompactAfterRescale =
                TernaryBoolean.mergeTernaryBooleanWithConfig(
                        original.incrementalRestoreAsyncCompactAfterRescale,
                        INCREMENTAL_RESTORE_ASYNC_COMPACT_AFTER_RESCALE,
                        config);

        useIngestDbRestoreMode =
                TernaryBoolean.mergeTernaryBooleanWithConfig(
                        original.useIngestDbRestoreMode, USE_INGEST_DB_RESTORE_MODE, config);

        rescalingUseDeleteFilesInRange =
                TernaryBoolean.mergeTernaryBooleanWithConfig(
                        original.rescalingUseDeleteFilesInRange,
                        USE_DELETE_FILES_IN_RANGE_DURING_RESCALING,
                        config);

        this.rocksDBMemoryFactory = original.rocksDBMemoryFactory;

        this.manualCompactionConfig =
                original.manualCompactionConfig != null
                        ? original.manualCompactionConfig
                        : RocksDBManualCompactionConfig.from(config);
    }

    // ------------------------------------------------------------------------
    //  Reconfiguration
    // ------------------------------------------------------------------------

    /**
     * Creates a copy of this state backend that uses the values defined in the configuration for
     * fields where that were not yet specified in this state backend.
     *
     * @param config The configuration.
     * @param classLoader The class loader.
     * @return The re-configured variant of the state backend
     */
    @Override
    public EmbeddedRocksDBStateBackend configure(ReadableConfig config, ClassLoader classLoader) {
        return new EmbeddedRocksDBStateBackend(this, config, classLoader);
    }

    // ------------------------------------------------------------------------
    //  State backend methods
    // ------------------------------------------------------------------------

    @Override
    public boolean supportsNoClaimRestoreMode() {
        // We are able to create CheckpointType#FULL_CHECKPOINT. (we might potentially reupload some
        // shared files when taking incremental snapshots)
        return true;
    }

    @Override
    public boolean supportsSavepointFormat(SavepointFormatType formatType) {
        return true;
    }

    private void lazyInitializeForJob(
            Environment env, @SuppressWarnings("unused") String operatorIdentifier)
            throws IOException {

        if (isInitialized) {
            return;
        }

        this.jobId = env.getJobID();

        // initialize the paths where the local RocksDB files should be stored
        if (localRocksDbDirectories == null) {
            initializedDbBasePaths = new File[] {env.getTaskManagerInfo().getTmpWorkingDirectory()};
        } else {
            List<File> dirs = new ArrayList<>(localRocksDbDirectories.length);
            StringBuilder errorMessage = new StringBuilder();

            for (File f : localRocksDbDirectories) {
                File testDir = new File(f, UUID.randomUUID().toString());
                if (!testDir.mkdirs()) {
                    String msg =
                            "Local DB files directory '"
                                    + f
                                    + "' does not exist and cannot be created. ";
                    LOG.error(msg);
                    errorMessage.append(msg);
                } else {
                    dirs.add(f);
                }
                //noinspection ResultOfMethodCallIgnored
                testDir.delete();
            }

            if (dirs.isEmpty()) {
                throw new IOException("No local storage directories available. " + errorMessage);
            } else {
                initializedDbBasePaths = dirs.toArray(new File[0]);
            }
        }

        nextDirectory = new Random().nextInt(initializedDbBasePaths.length);

        isInitialized = true;
    }

    private File getNextStoragePath() {
        int ni = nextDirectory + 1;
        ni = ni >= initializedDbBasePaths.length ? 0 : ni;
        nextDirectory = ni;

        return initializedDbBasePaths[ni];
    }

    // ------------------------------------------------------------------------
    //  State holding data structures
    // ------------------------------------------------------------------------

    @Override
    public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
            KeyedStateBackendParameters<K> parameters) throws IOException {
        Environment env = parameters.getEnv();

        // first, make sure that the RocksDB JNI library is loaded
        // we do this explicitly here to have better error handling
        String tempDir = env.getTaskManagerInfo().getTmpWorkingDirectory().getAbsolutePath();
        ensureRocksDBIsLoaded(tempDir);

        // replace all characters that are not legal for filenames with underscore
        String fileCompatibleIdentifier =
                parameters.getOperatorIdentifier().replaceAll("[^a-zA-Z0-9\\-]", "_");

        lazyInitializeForJob(env, fileCompatibleIdentifier);

        File instanceBasePath =
                new File(
                        getNextStoragePath(),
                        "job_"
                                + jobId
                                + "_op_"
                                + fileCompatibleIdentifier
                                + "_uuid_"
                                + UUID.randomUUID());

        LocalRecoveryConfig localRecoveryConfig =
                env.getTaskStateManager().createLocalRecoveryConfig();

        final OpaqueMemoryResource<RocksDBSharedResources> sharedResources =
                RocksDBOperationUtils.allocateSharedCachesIfConfigured(
                        memoryConfiguration,
                        env,
                        parameters.getManagedMemoryFraction(),
                        LOG,
                        rocksDBMemoryFactory);
        if (sharedResources != null) {
            LOG.info("Obtained shared RocksDB cache of size {} bytes", sharedResources.getSize());
        }
        final RocksDBResourceContainer resourceContainer =
                createOptionsAndResourceContainer(
                        sharedResources,
                        instanceBasePath,
                        nativeMetricOptions.isStatisticsEnabled());

        ExecutionConfig executionConfig = env.getExecutionConfig();
        StreamCompressionDecorator keyGroupCompressionDecorator =
                getCompressionDecorator(executionConfig);

        LatencyTrackingStateConfig latencyTrackingStateConfig =
                latencyTrackingConfigBuilder.setMetricGroup(parameters.getMetricGroup()).build();
        RocksDBKeyedStateBackendBuilder<K> builder =
                new RocksDBKeyedStateBackendBuilder<>(
                                parameters.getOperatorIdentifier(),
                                env.getUserCodeClassLoader().asClassLoader(),
                                instanceBasePath,
                                resourceContainer,
                                stateName -> resourceContainer.getColumnOptions(),
                                parameters.getKvStateRegistry(),
                                parameters.getKeySerializer(),
                                parameters.getNumberOfKeyGroups(),
                                parameters.getKeyGroupRange(),
                                executionConfig,
                                localRecoveryConfig,
                                priorityQueueConfig,
                                parameters.getTtlTimeProvider(),
                                latencyTrackingStateConfig,
                                parameters.getMetricGroup(),
                                parameters.getCustomInitializationMetrics(),
                                parameters.getStateHandles(),
                                keyGroupCompressionDecorator,
                                parameters.getCancelStreamRegistry())
                        .setEnableIncrementalCheckpointing(isIncrementalCheckpointsEnabled())
                        .setNumberOfTransferingThreads(getNumberOfTransferThreads())
                        .setNativeMetricOptions(
                                resourceContainer.getMemoryWatcherOptions(nativeMetricOptions))
                        .setWriteBatchSize(getWriteBatchSize())
                        .setOverlapFractionThreshold(getOverlapFractionThreshold())
                        .setIncrementalRestoreAsyncCompactAfterRescale(
                                getIncrementalRestoreAsyncCompactAfterRescale())
                        .setUseIngestDbRestoreMode(getUseIngestDbRestoreMode())
                        .setRescalingUseDeleteFilesInRange(isRescalingUseDeleteFilesInRange())
                        .setIOExecutor(
                                MdcUtils.scopeToJob(
                                        jobId,
                                        parameters.getEnv().getIOManager().getExecutorService()))
                        .setManualCompactionConfig(
                                manualCompactionConfig == null
                                        ? RocksDBManualCompactionConfig.getDefault()
                                        : manualCompactionConfig)
                        .setAsyncExceptionHandler(
                                (ign, throwable) -> parameters.getEnv().failExternally(throwable));
        return builder.build();
    }

    @Override
    public OperatorStateBackend createOperatorStateBackend(
            OperatorStateBackendParameters parameters) throws Exception {
        // the default for RocksDB; eventually there can be a operator state backend based on
        // RocksDB, too.
        final boolean asyncSnapshots = true;
        return new DefaultOperatorStateBackendBuilder(
                        parameters.getEnv().getUserCodeClassLoader().asClassLoader(),
                        parameters.getEnv().getExecutionConfig(),
                        asyncSnapshots,
                        parameters.getStateHandles(),
                        parameters.getCancelStreamRegistry())
                .build();
    }

    private RocksDBOptionsFactory configureOptionsFactory(
            @Nullable RocksDBOptionsFactory originalOptionsFactory,
            @Nullable String factoryClassName,
            ReadableConfig config,
            ClassLoader classLoader)
            throws DynamicCodeLoadingException {

        RocksDBOptionsFactory optionsFactory = null;

        if (originalOptionsFactory != null) {
            if (originalOptionsFactory instanceof ConfigurableRocksDBOptionsFactory) {
                originalOptionsFactory =
                        ((ConfigurableRocksDBOptionsFactory) originalOptionsFactory)
                                .configure(config);
            }
            LOG.info("Using application-defined options factory: {}.", originalOptionsFactory);

            optionsFactory = originalOptionsFactory;
        } else if (factoryClassName != null) {
            // Do nothing if user does not define any factory class.
            try {
                Class<? extends RocksDBOptionsFactory> clazz =
                        Class.forName(factoryClassName, false, classLoader)
                                .asSubclass(RocksDBOptionsFactory.class);

                optionsFactory = clazz.newInstance();
                if (optionsFactory instanceof ConfigurableRocksDBOptionsFactory) {
                    optionsFactory =
                            ((ConfigurableRocksDBOptionsFactory) optionsFactory).configure(config);
                }
                LOG.info("Using configured options factory: {}.", optionsFactory);

            } catch (ClassNotFoundException e) {
                throw new DynamicCodeLoadingException(
                        "Cannot find configured options factory class: " + factoryClassName, e);
            } catch (ClassCastException | InstantiationException | IllegalAccessException e) {
                throw new DynamicCodeLoadingException(
                        "The class configured under '"
                                + RocksDBOptions.OPTIONS_FACTORY.key()
                                + "' is not a valid options factory ("
                                + factoryClassName
                                + ')',
                        e);
            }
        }
        return optionsFactory;
    }

    // ------------------------------------------------------------------------
    //  Parameters
    // ------------------------------------------------------------------------

    /**
     * Gets the memory configuration object, which offers settings to control RocksDB's memory
     * usage.
     */
    public RocksDBMemoryConfiguration getMemoryConfiguration() {
        return memoryConfiguration;
    }

    /**
     * Sets the path where the RocksDB local database files should be stored on the local file
     * system. Setting this path overrides the default behavior, where the files are stored across
     * the configured temp directories.
     *
     * <p>Passing {@code null} to this function restores the default behavior, where the configured
     * temp directories will be used.
     *
     * @param path The path where the local RocksDB database files are stored.
     */
    public void setDbStoragePath(String path) {
        setDbStoragePaths(path == null ? null : new String[] {path});
    }

    /**
     * Sets the directories in which the local RocksDB database puts its files (like SST and
     * metadata files). These directories do not need to be persistent, they can be ephemeral,
     * meaning that they are lost on a machine failure, because state in RocksDB is persisted in
     * checkpoints.
     *
     * <p>If nothing is configured, these directories default to the TaskManager's local temporary
     * file directories.
     *
     * <p>Each distinct state will be stored in one path, but when the state backend creates
     * multiple states, they will store their files on different paths.
     *
     * <p>Passing {@code null} to this function restores the default behavior, where the configured
     * temp directories will be used.
     *
     * @param paths The paths across which the local RocksDB database files will be spread.
     */
    public void setDbStoragePaths(String... paths) {
        if (paths == null) {
            localRocksDbDirectories = null;
        } else if (paths.length == 0) {
            throw new IllegalArgumentException("empty paths");
        } else {
            File[] pp = new File[paths.length];

            for (int i = 0; i < paths.length; i++) {
                final String rawPath = paths[i];
                final String path;

                if (rawPath == null) {
                    throw new IllegalArgumentException("null path");
                } else {
                    // we need this for backwards compatibility, to allow URIs like 'file:///'...
                    URI uri = null;
                    try {
                        uri = new Path(rawPath).toUri();
                    } catch (Exception e) {
                        // cannot parse as a path
                    }

                    if (uri != null && uri.getScheme() != null) {
                        if ("file".equalsIgnoreCase(uri.getScheme())) {
                            path = uri.getPath();
                        } else {
                            throw new IllegalArgumentException(
                                    "Path " + rawPath + " has a non-local scheme");
                        }
                    } else {
                        path = rawPath;
                    }
                }

                pp[i] = new File(path);
                if (!pp[i].isAbsolute()) {
                    throw new IllegalArgumentException("Relative paths are not supported");
                }
            }

            localRocksDbDirectories = pp;
        }
    }

    /**
     * Gets the configured local DB storage paths, or null, if none were configured.
     *
     * <p>Under these directories on the TaskManager, RocksDB stores its SST files and metadata
     * files. These directories do not need to be persistent, they can be ephermeral, meaning that
     * they are lost on a machine failure, because state in RocksDB is persisted in checkpoints.
     *
     * <p>If nothing is configured, these directories default to the TaskManager's local temporary
     * file directories.
     */
    public String[] getDbStoragePaths() {
        if (localRocksDbDirectories == null) {
            return null;
        } else {
            String[] paths = new String[localRocksDbDirectories.length];
            for (int i = 0; i < paths.length; i++) {
                paths[i] = localRocksDbDirectories[i].toString();
            }
            return paths;
        }
    }

    /** Gets whether incremental checkpoints are enabled for this state backend. */
    public boolean isIncrementalCheckpointsEnabled() {
        return enableIncrementalCheckpointing.getOrDefault(
                CheckpointingOptions.INCREMENTAL_CHECKPOINTS.defaultValue());
    }

    /**
     * Gets the type of the priority queue state. It will fallback to the default value, if it is
     * not explicitly set.
     *
     * @return The type of the priority queue state.
     */
    public PriorityQueueStateType getPriorityQueueStateType() {
        return priorityQueueConfig.getPriorityQueueStateType();
    }

    /**
     * Sets the type of the priority queue state. It will fallback to the default value, if it is
     * not explicitly set.
     */
    public void setPriorityQueueStateType(PriorityQueueStateType priorityQueueStateType) {
        this.priorityQueueConfig.setPriorityQueueStateType(priorityQueueStateType);
    }

    // ------------------------------------------------------------------------
    //  Parametrize with RocksDB Options
    // ------------------------------------------------------------------------

    /**
     * Sets the predefined options for RocksDB.
     *
     * <p>If user-configured options within {@link RocksDBConfigurableOptions} is set (through
     * config.yaml) or a user-defined options factory is set (via {@link
     * #setRocksDBOptions(RocksDBOptionsFactory)}), then the options from the factory are applied on
     * top of the here specified predefined options and customized options.
     *
     * @param options The options to set (must not be null).
     */
    public void setPredefinedOptions(@Nonnull PredefinedOptions options) {
        predefinedOptions = checkNotNull(options);
    }

    /**
     * Gets the currently set predefined options for RocksDB. The default options (if nothing was
     * set via {@link #setPredefinedOptions(PredefinedOptions)}) are {@link
     * PredefinedOptions#DEFAULT}.
     *
     * <p>If user-configured options within {@link RocksDBConfigurableOptions} is set (through
     * config.yaml) of a user-defined options factory is set (via {@link
     * #setRocksDBOptions(RocksDBOptionsFactory)}), then the options from the factory are applied on
     * top of the predefined and customized options.
     *
     * @return The currently set predefined options for RocksDB.
     */
    @VisibleForTesting
    public PredefinedOptions getPredefinedOptions() {
        if (predefinedOptions == null) {
            predefinedOptions = PredefinedOptions.DEFAULT;
        }
        return predefinedOptions;
    }

    /**
     * Sets {@link org.rocksdb.Options} for the RocksDB instances. Because the options are not
     * serializable and hold native code references, they must be specified through a factory.
     *
     * <p>The options created by the factory here are applied on top of the pre-defined options
     * profile selected via {@link #setPredefinedOptions(PredefinedOptions)} and user-configured
     * options from configuration set by {@link #configure(ReadableConfig, ClassLoader)} with keys
     * in {@link RocksDBConfigurableOptions}.
     *
     * @param optionsFactory The options factory that lazily creates the RocksDB options.
     */
    public void setRocksDBOptions(RocksDBOptionsFactory optionsFactory) {
        this.rocksDbOptionsFactory = optionsFactory;
    }

    /**
     * Gets {@link org.rocksdb.Options} for the RocksDB instances.
     *
     * <p>The options created by the factory here are applied on top of the pre-defined options
     * profile selected via {@link #setPredefinedOptions(PredefinedOptions)}. If the pre-defined
     * options profile is the default ({@link PredefinedOptions#DEFAULT}), then the factory fully
     * controls the RocksDB options.
     */
    @Nullable
    public RocksDBOptionsFactory getRocksDBOptions() {
        return rocksDbOptionsFactory;
    }

    /** Gets the number of threads used to transfer files while snapshotting/restoring. */
    public int getNumberOfTransferThreads() {
        return numberOfTransferThreads == UNDEFINED_NUMBER_OF_TRANSFER_THREADS
                ? CHECKPOINT_TRANSFER_THREAD_NUM.defaultValue()
                : numberOfTransferThreads;
    }

    /**
     * Sets the number of threads used to transfer files while snapshotting/restoring.
     *
     * @param numberOfTransferThreads The number of threads used to transfer files while
     *     snapshotting/restoring.
     */
    public void setNumberOfTransferThreads(int numberOfTransferThreads) {
        Preconditions.checkArgument(
                numberOfTransferThreads > 0,
                "The number of threads used to transfer files in EmbeddedRocksDBStateBackend should be greater than zero.");
        this.numberOfTransferThreads = numberOfTransferThreads;
    }

    /** Gets the max batch size will be used in {@link RocksDBWriteBatchWrapper}. */
    public long getWriteBatchSize() {
        return writeBatchSize == UNDEFINED_WRITE_BATCH_SIZE
                ? WRITE_BATCH_SIZE.defaultValue().getBytes()
                : writeBatchSize;
    }

    /**
     * Sets the max batch size will be used in {@link RocksDBWriteBatchWrapper}, no positive value
     * will disable memory size controller, just use item count controller.
     *
     * @param writeBatchSize The size will used to be used in {@link RocksDBWriteBatchWrapper}.
     */
    public void setWriteBatchSize(long writeBatchSize) {
        checkArgument(writeBatchSize >= 0, "Write batch size have to be no negative.");
        this.writeBatchSize = writeBatchSize;
    }

    /** Set RocksDBMemoryFactory. */
    public void setRocksDBMemoryFactory(RocksDBMemoryFactory rocksDBMemoryFactory) {
        this.rocksDBMemoryFactory = checkNotNull(rocksDBMemoryFactory);
    }

    double getOverlapFractionThreshold() {
        return overlapFractionThreshold == UNDEFINED_OVERLAP_FRACTION_THRESHOLD
                ? RESTORE_OVERLAP_FRACTION_THRESHOLD.defaultValue()
                : overlapFractionThreshold;
    }

    boolean getIncrementalRestoreAsyncCompactAfterRescale() {
        return incrementalRestoreAsyncCompactAfterRescale.getOrDefault(
                INCREMENTAL_RESTORE_ASYNC_COMPACT_AFTER_RESCALE.defaultValue());
    }

    boolean getUseIngestDbRestoreMode() {
        return useIngestDbRestoreMode.getOrDefault(USE_INGEST_DB_RESTORE_MODE.defaultValue());
    }

    boolean isRescalingUseDeleteFilesInRange() {
        return rescalingUseDeleteFilesInRange.getOrDefault(
                USE_DELETE_FILES_IN_RANGE_DURING_RESCALING.defaultValue());
    }

    // ------------------------------------------------------------------------
    //  utilities
    // ------------------------------------------------------------------------

    private ReadableConfig mergeConfigurableOptions(ReadableConfig base, ReadableConfig onTop) {
        if (base == null) {
            base = new Configuration();
        }
        Configuration configuration = new Configuration();
        Map<String, String> baseMap = base.toMap();
        Map<String, String> onTopMap = onTop.toMap();
        for (ConfigOption<?> option : RocksDBConfigurableOptions.CANDIDATE_CONFIGS) {
            Optional<?> baseValue = base.getOptional(option);
            Optional<?> topValue = onTop.getOptional(option);

            if (topValue.isPresent() || baseValue.isPresent()) {
                Object validValue = topValue.isPresent() ? topValue.get() : baseValue.get();
                RocksDBConfigurableOptions.checkArgumentValid(option, validValue);
                String valueString =
                        topValue.isPresent()
                                ? onTopMap.get(option.key())
                                : baseMap.get(option.key());
                configuration.setString(option.key(), valueString);
            }
        }
        return configuration;
    }

    @VisibleForTesting
    RocksDBResourceContainer createOptionsAndResourceContainer(@Nullable File instanceBasePath) {
        return createOptionsAndResourceContainer(null, instanceBasePath, false);
    }

    @VisibleForTesting
    private RocksDBResourceContainer createOptionsAndResourceContainer(
            @Nullable OpaqueMemoryResource<RocksDBSharedResources> sharedResources,
            @Nullable File instanceBasePath,
            boolean enableStatistics) {

        return new RocksDBResourceContainer(
                configurableOptions != null ? configurableOptions : new Configuration(),
                predefinedOptions != null ? predefinedOptions : PredefinedOptions.DEFAULT,
                rocksDbOptionsFactory,
                sharedResources,
                instanceBasePath,
                enableStatistics);
    }

    @Override
    public String toString() {
        return "EmbeddedRocksDBStateBackend{"
                + ", localRocksDbDirectories="
                + Arrays.toString(localRocksDbDirectories)
                + ", enableIncrementalCheckpointing="
                + enableIncrementalCheckpointing
                + ", numberOfTransferThreads="
                + numberOfTransferThreads
                + ", writeBatchSize="
                + writeBatchSize
                + '}';
    }

    // ------------------------------------------------------------------------
    //  static library loading utilities
    // ------------------------------------------------------------------------

    @VisibleForTesting
    static void ensureRocksDBIsLoaded(String tempDirectory) throws IOException {
        ensureRocksDBIsLoaded(tempDirectory, NativeLibraryLoader::getInstance);
    }

    @VisibleForTesting
    static void ensureRocksDBIsLoaded(
            String tempDirectory, Supplier<NativeLibraryLoader> nativeLibraryLoaderSupplier)
            throws IOException {
        synchronized (EmbeddedRocksDBStateBackend.class) {
            if (!rocksDbInitialized) {

                final File tempDirParent = new File(tempDirectory).getAbsoluteFile();
                LOG.info(
                        "Attempting to load RocksDB native library and store it under '{}'",
                        tempDirParent);

                Throwable lastException = null;
                for (int attempt = 1; attempt <= ROCKSDB_LIB_LOADING_ATTEMPTS; attempt++) {
                    File rocksLibFolder = null;
                    try {
                        // when multiple instances of this class and RocksDB exist in different
                        // class loaders, then we can see the following exception:
                        // "java.lang.UnsatisfiedLinkError: Native Library
                        // /path/to/temp/dir/librocksdbjni-linux64.so
                        // already loaded in another class loader"

                        // to avoid that, we need to add a random element to the library file path
                        // (I know, seems like an unnecessary hack, since the JVM obviously can
                        // handle multiple
                        //  instances of the same JNI library being loaded in different class
                        // loaders, but
                        //  apparently not when coming from the same file path, so there we go)

                        rocksLibFolder = new File(tempDirParent, "rocksdb-lib-" + new AbstractID());

                        // make sure the temp path exists
                        LOG.debug(
                                "Attempting to create RocksDB native library folder {}",
                                rocksLibFolder);
                        // noinspection ResultOfMethodCallIgnored
                        rocksLibFolder.mkdirs();

                        // explicitly load the JNI dependency if it has not been loaded before
                        nativeLibraryLoaderSupplier
                                .get()
                                .loadLibrary(rocksLibFolder.getAbsolutePath());

                        // this initialization here should validate that the loading succeeded
                        RocksDB.loadLibrary();

                        // seems to have worked
                        LOG.info("Successfully loaded RocksDB native library");
                        rocksDbInitialized = true;
                        return;
                    } catch (Throwable t) {
                        lastException = t;
                        LOG.debug("RocksDB JNI library loading attempt {} failed", attempt, t);

                        // try to force RocksDB to attempt reloading the library
                        try {
                            resetRocksDBLoadedFlag();
                        } catch (Throwable tt) {
                            LOG.debug(
                                    "Failed to reset 'initialized' flag in RocksDB native code loader",
                                    tt);
                        }

                        FileUtils.deleteDirectoryQuietly(rocksLibFolder);
                    }
                }

                throw new IOException("Could not load the native RocksDB library", lastException);
            }
        }
    }

    @VisibleForTesting
    static void resetRocksDBLoadedFlag() throws Exception {
        final Field initField =
                org.rocksdb.NativeLibraryLoader.class.getDeclaredField("initialized");
        initField.setAccessible(true);
        initField.setBoolean(null, false);
    }

    // ---------------------------------------------------------------------------------------------
    // Enums
    // ---------------------------------------------------------------------------------------------

    /** The options to chose for the type of priority queue state. */
    public enum PriorityQueueStateType implements DescribedEnum {
        HEAP(text("Heap-based")),
        ROCKSDB(text("Implementation based on RocksDB"));

        private final InlineElement description;

        PriorityQueueStateType(InlineElement description) {
            this.description = description;
        }

        @Override
        public InlineElement getDescription() {
            return description;
        }
    }
}
