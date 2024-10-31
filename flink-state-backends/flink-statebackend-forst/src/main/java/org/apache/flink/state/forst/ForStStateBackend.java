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

package org.apache.flink.state.forst;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.description.InlineElement;
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
import org.apache.flink.state.forst.ForStMemoryControllerUtils.ForStMemoryFactory;
import org.apache.flink.state.forst.sync.ForStPriorityQueueConfig;
import org.apache.flink.state.forst.sync.ForStSyncKeyedStateBackendBuilder;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.DynamicCodeLoadingException;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.concurrent.FutureUtils;

import org.forstdb.NativeLibraryLoader;
import org.forstdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.apache.flink.configuration.description.TextElement.text;

/**
 * A {@link org.apache.flink.runtime.state.StateBackend} that stores its state in a ForSt instance.
 * This state backend can store very large state that exceeds memory even disk and spills to remote
 * storage.
 *
 * <p>The behavior of the ForSt instances can be parametrized by setting ForSt Options using the
 * methods {@link #setForStOptions(ForStOptionsFactory)}.
 */
@Experimental
public class ForStStateBackend extends AbstractManagedMemoryStateBackend
        implements ConfigurableStateBackend {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ForStStateBackend.class);

    /** The number of (re)tries for loading the ForSt JNI library. */
    private static final int FORST_LIB_LOADING_ATTEMPTS = 3;

    /** Flag whether the native library has been loaded. */
    private static boolean forStInitialized = false;

    // ------------------------------------------------------------------------

    // -- configuration values, set in the application / configuration

    /**
     * Base paths for ForSt remote directory, as configured. Null if not yet set, in which case the
     * configuration values will be used. The configuration will fallback to local directory by
     * default. TODO: fallback to checkpoint directory if not configured.
     */
    @Nullable private Path remoteForStDirectory;

    /**
     * Base paths for ForSt directory, as configured. Null if not yet set, in which case the
     * configuration values will be used. The configuration defaults to the TaskManager's temp
     * directories.
     */
    @Nullable private File[] localForStDirectories;

    /** The configurable options. */
    @Nullable private ReadableConfig configurableOptions;

    /** The options factory to create the ForSt options in the cluster. */
    @Nullable private ForStOptionsFactory forStOptionsFactory;

    /** The configuration for memory settings (pool sizes, etc.). */
    private final ForStMemoryConfiguration memoryConfiguration;

    /** The default ForSt property-based metrics options. */
    private final ForStNativeMetricOptions nativeMetricOptions;

    // -- runtime values, set on TaskManager when initializing / using the backend

    /** Base paths for ForSt directory, as initialized. */
    private transient File[] initializedDbBasePaths;

    /** JobID for uniquifying backup paths. */
    private transient JobID jobId;

    /** The index of the next directory to be used from {@link #initializedDbBasePaths}. */
    private transient int nextDirectory;

    /** Whether we already lazily initialized our local storage directories. */
    private transient boolean isInitialized;

    /** Factory for Write Buffer Manager and Block Cache. */
    private final ForStMemoryFactory forStMemoryFactory;

    /**
     * The configuration for rocksdb priorityQueue state settings (priorityQueue state type, etc.).
     */
    private final ForStPriorityQueueConfig priorityQueueConfig;
    // ------------------------------------------------------------------------

    /** Creates a new {@code ForStStateBackend} for storing state. */
    public ForStStateBackend() {
        this.nativeMetricOptions = new ForStNativeMetricOptions();
        this.memoryConfiguration = new ForStMemoryConfiguration();
        this.priorityQueueConfig = new ForStPriorityQueueConfig();
        this.forStMemoryFactory = ForStMemoryFactory.DEFAULT;
    }

    /**
     * Private constructor that creates a re-configured copy of the state backend.
     *
     * @param original The state backend to re-configure.
     * @param config The configuration.
     * @param classLoader The class loader.
     */
    private ForStStateBackend(
            ForStStateBackend original, ReadableConfig config, ClassLoader classLoader) {
        this.memoryConfiguration =
                ForStMemoryConfiguration.fromOtherAndConfiguration(
                        original.memoryConfiguration, config);
        this.memoryConfiguration.validate();

        if (original.remoteForStDirectory != null) {
            this.remoteForStDirectory = original.remoteForStDirectory;
        } else {
            String remoteDirStr = config.get(ForStOptions.REMOTE_DIRECTORY);
            this.remoteForStDirectory = remoteDirStr == null ? null : new Path(remoteDirStr);
        }

        this.priorityQueueConfig =
                ForStPriorityQueueConfig.fromOtherAndConfiguration(
                        original.priorityQueueConfig, config);

        // configure local directories
        if (original.localForStDirectories != null) {
            this.localForStDirectories = original.localForStDirectories;
        } else {
            final String forStLocalPaths = config.get(ForStOptions.LOCAL_DIRECTORIES);
            if (forStLocalPaths != null) {
                String[] directories = forStLocalPaths.split(",|" + File.pathSeparator);

                try {
                    setLocalDbStoragePaths(directories);
                } catch (IllegalArgumentException e) {
                    throw new IllegalConfigurationException(
                            "Invalid configuration for ForSt state "
                                    + "backend's local storage directories: "
                                    + e.getMessage(),
                            e);
                }
            }
        }

        // configure metric options
        this.nativeMetricOptions = ForStNativeMetricOptions.fromConfig(config);

        // configurable options
        this.configurableOptions = mergeConfigurableOptions(original.configurableOptions, config);

        // configure ForSt options factory
        try {
            forStOptionsFactory =
                    configureOptionsFactory(
                            original.forStOptionsFactory,
                            config.get(ForStOptions.OPTIONS_FACTORY),
                            config,
                            classLoader);
        } catch (DynamicCodeLoadingException e) {
            throw new FlinkRuntimeException(e);
        }

        // configure latency tracking
        latencyTrackingConfigBuilder = original.latencyTrackingConfigBuilder.configure(config);

        this.forStMemoryFactory = original.forStMemoryFactory;
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
    public ForStStateBackend configure(ReadableConfig config, ClassLoader classLoader) {
        return new ForStStateBackend(this, config, classLoader);
    }

    // ------------------------------------------------------------------------
    //  State backend methods
    // ------------------------------------------------------------------------

    private void lazyInitializeForJob(
            Environment env, @SuppressWarnings("unused") String operatorIdentifier)
            throws IOException {

        if (isInitialized) {
            return;
        }

        this.jobId = env.getJobID();

        // initialize the paths where the local ForSt files should be stored
        if (localForStDirectories == null) {
            initializedDbBasePaths = new File[] {env.getTaskManagerInfo().getTmpWorkingDirectory()};
        } else {
            List<File> dirs = new ArrayList<>(localForStDirectories.length);
            StringBuilder errorMessage = new StringBuilder();

            for (File f : localForStDirectories) {
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
    public boolean supportsAsyncKeyedStateBackend() {
        return true;
    }

    @Override
    public <K> ForStKeyedStateBackend<K> createAsyncKeyedStateBackend(
            KeyedStateBackendParameters<K> parameters) throws IOException {
        Environment env = parameters.getEnv();

        // first, make sure that the ForSt JNI library is loaded
        // we do this explicitly here to have better error handling
        String tempDir = env.getTaskManagerInfo().getTmpWorkingDirectory().getAbsolutePath();
        ensureForStIsLoaded(tempDir, env.getAsyncOperationsThreadPool());

        // replace all characters that are not legal for filenames with underscore
        String fileCompatibleIdentifier =
                parameters.getOperatorIdentifier().replaceAll("[^a-zA-Z0-9\\-]", "_");

        lazyInitializeForJob(env, fileCompatibleIdentifier);

        String opChildPath =
                String.format(
                        "op_%s_attempt_%s",
                        fileCompatibleIdentifier, env.getTaskInfo().getAttemptNumber());

        File localBasePath =
                new File(new File(getNextStoragePath(), jobId.toHexString()), opChildPath);
        Path remoteBasePath =
                remoteForStDirectory != null
                        ? new Path(new Path(remoteForStDirectory, jobId.toHexString()), opChildPath)
                        : null;

        final OpaqueMemoryResource<ForStSharedResources> sharedResources =
                ForStOperationUtils.allocateSharedCachesIfConfigured(
                        memoryConfiguration,
                        env,
                        parameters.getManagedMemoryFraction(),
                        LOG,
                        forStMemoryFactory);
        if (sharedResources != null) {
            LOG.info("Obtained shared ForSt cache of size {} bytes", sharedResources.getSize());
        }
        final ForStResourceContainer resourceContainer =
                createOptionsAndResourceContainer(
                        sharedResources,
                        localBasePath,
                        remoteBasePath,
                        nativeMetricOptions.isStatisticsEnabled());

        ForStKeyedStateBackendBuilder<K> builder =
                new ForStKeyedStateBackendBuilder<>(
                                parameters.getOperatorIdentifier(),
                                env.getUserCodeClassLoader().asClassLoader(),
                                resourceContainer,
                                stateName -> resourceContainer.getColumnOptions(),
                                parameters.getKeySerializer(),
                                parameters.getNumberOfKeyGroups(),
                                parameters.getKeyGroupRange(),
                                priorityQueueConfig,
                                parameters.getTtlTimeProvider(),
                                parameters.getMetricGroup(),
                                parameters.getCustomInitializationMetrics(),
                                parameters.getStateHandles(),
                                parameters.getCancelStreamRegistry())
                        // TODO: remove after support more snapshot strategy
                        .setEnableIncrementalCheckpointing(true)
                        .setNativeMetricOptions(
                                resourceContainer.getMemoryWatcherOptions(nativeMetricOptions));
        return builder.build();
    }

    @Override
    public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
            KeyedStateBackendParameters<K> parameters) throws IOException {
        Environment env = parameters.getEnv();

        // first, make sure that the RocksDB JNI library is loaded
        // we do this explicitly here to have better error handling
        String tempDir = env.getTaskManagerInfo().getTmpWorkingDirectory().getAbsolutePath();
        ensureForStIsLoaded(tempDir, env.getAsyncOperationsThreadPool());

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

        final OpaqueMemoryResource<ForStSharedResources> sharedResources =
                ForStOperationUtils.allocateSharedCachesIfConfigured(
                        memoryConfiguration,
                        env,
                        parameters.getManagedMemoryFraction(),
                        LOG,
                        forStMemoryFactory);
        if (sharedResources != null) {
            LOG.info("Obtained shared RocksDB cache of size {} bytes", sharedResources.getSize());
        }
        final ForStResourceContainer resourceContainer =
                createOptionsAndResourceContainer(
                        sharedResources,
                        instanceBasePath,
                        null,
                        nativeMetricOptions.isStatisticsEnabled());

        ExecutionConfig executionConfig = env.getExecutionConfig();
        StreamCompressionDecorator keyGroupCompressionDecorator =
                getCompressionDecorator(executionConfig);

        LatencyTrackingStateConfig latencyTrackingStateConfig =
                latencyTrackingConfigBuilder.setMetricGroup(parameters.getMetricGroup()).build();
        ForStSyncKeyedStateBackendBuilder<K> builder =
                new ForStSyncKeyedStateBackendBuilder<>(
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
                        .setNativeMetricOptions(
                                resourceContainer.getMemoryWatcherOptions(nativeMetricOptions));
        return builder.build();
    }

    @Override
    public OperatorStateBackend createOperatorStateBackend(
            OperatorStateBackendParameters parameters) throws Exception {
        // the default for ForSt; eventually there can be a operator state backend based on
        // ForSt, too.
        final boolean asyncSnapshots = true;
        return new DefaultOperatorStateBackendBuilder(
                        parameters.getEnv().getUserCodeClassLoader().asClassLoader(),
                        parameters.getEnv().getExecutionConfig(),
                        asyncSnapshots,
                        parameters.getStateHandles(),
                        parameters.getCancelStreamRegistry())
                .build();
    }

    private ForStOptionsFactory configureOptionsFactory(
            @Nullable ForStOptionsFactory originalOptionsFactory,
            @Nullable String factoryClassName,
            ReadableConfig config,
            ClassLoader classLoader)
            throws DynamicCodeLoadingException {

        ForStOptionsFactory optionsFactory = null;

        if (originalOptionsFactory != null) {
            if (originalOptionsFactory instanceof ConfigurableForStOptionsFactory) {
                originalOptionsFactory =
                        ((ConfigurableForStOptionsFactory) originalOptionsFactory)
                                .configure(config);
            }
            LOG.info("Using application-defined options factory: {}.", originalOptionsFactory);

            optionsFactory = originalOptionsFactory;
        } else if (factoryClassName != null) {
            try {
                Class<? extends ForStOptionsFactory> clazz =
                        Class.forName(factoryClassName, false, classLoader)
                                .asSubclass(ForStOptionsFactory.class);

                optionsFactory = clazz.newInstance();
                if (optionsFactory instanceof ConfigurableForStOptionsFactory) {
                    optionsFactory =
                            ((ConfigurableForStOptionsFactory) optionsFactory).configure(config);
                }
                LOG.info("Using configured options factory: {}.", optionsFactory);

            } catch (ClassNotFoundException e) {
                throw new DynamicCodeLoadingException(
                        "Cannot find configured options factory class: " + factoryClassName, e);
            } catch (ClassCastException | InstantiationException | IllegalAccessException e) {
                throw new DynamicCodeLoadingException(
                        "The class configured under '"
                                + ForStOptions.OPTIONS_FACTORY.key()
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
     * Sets the path where the ForSt local files should be stored on the local file system. Setting
     * this path overrides the default behavior, where the files are stored across the configured
     * temp directories.
     *
     * <p>Passing {@code null} to this function restores the default behavior, where the configured
     * temp directories will be used.
     *
     * @param path The path where the local ForSt database files are stored.
     */
    public void setLocalDbStoragePath(String path) {
        setLocalDbStoragePaths(path == null ? null : new String[] {path});
    }

    /**
     * Sets the local directories in which the ForSt database puts some files (like metadata files).
     * These directories do not need to be persistent, they can be ephemeral, meaning that they are
     * lost on a machine failure, because state in ForSt is persisted in checkpoints.
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
     * @param paths The paths across which the local ForSt database files will be spread.
     */
    public void setLocalDbStoragePaths(String... paths) {
        if (paths == null) {
            localForStDirectories = null;
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

            localForStDirectories = pp;
        }
    }

    /**
     * Gets the configured local DB storage paths, or null, if none were configured.
     *
     * <p>Under these directories on the TaskManager, ForSt stores some metadata files. These
     * directories do not need to be persistent, they can be ephermeral, meaning that they are lost
     * on a machine failure, because state in ForSt is persisted in checkpoints.
     *
     * <p>If nothing is configured, these directories default to the TaskManager's local temporary
     * file directories.
     */
    public String[] getLocalDbStoragePaths() {
        if (localForStDirectories == null) {
            return null;
        } else {
            String[] paths = new String[localForStDirectories.length];
            for (int i = 0; i < paths.length; i++) {
                paths[i] = localForStDirectories[i].toString();
            }
            return paths;
        }
    }

    // ------------------------------------------------------------------------
    //  Parametrize with ForSt Options
    // ------------------------------------------------------------------------

    /**
     * Sets {@link org.forstdb.Options} for the ForSt instances. Because the options are not
     * serializable and hold native code references, they must be specified through a factory.
     *
     * <p>The options created by the factory here are applied on top of user-configured options from
     * configuration set by {@link #configure(ReadableConfig, ClassLoader)} with keys in {@link
     * ForStConfigurableOptions}.
     *
     * @param optionsFactory The options factory that lazily creates the ForSt options.
     */
    public void setForStOptions(ForStOptionsFactory optionsFactory) {
        this.forStOptionsFactory = optionsFactory;
    }

    /** Gets {@link org.forstdb.Options} for the ForSt instances. */
    @Nullable
    public ForStOptionsFactory getForStOptions() {
        return forStOptionsFactory;
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
        for (ConfigOption<?> option : ForStConfigurableOptions.CANDIDATE_CONFIGS) {
            Optional<?> baseValue = base.getOptional(option);
            Optional<?> topValue = onTop.getOptional(option);

            if (topValue.isPresent() || baseValue.isPresent()) {
                Object validValue = topValue.isPresent() ? topValue.get() : baseValue.get();
                ForStConfigurableOptions.checkArgumentValid(option, validValue);
                configuration.setString(option.key(), validValue.toString());
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
    ForStResourceContainer createOptionsAndResourceContainer(@Nullable File localBasePath) {
        return createOptionsAndResourceContainer(null, localBasePath, null, false);
    }

    @VisibleForTesting
    private ForStResourceContainer createOptionsAndResourceContainer(
            @Nullable OpaqueMemoryResource<ForStSharedResources> sharedResources,
            @Nullable File localBasePath,
            @Nullable Path remoteBasePath,
            boolean enableStatistics) {

        return new ForStResourceContainer(
                configurableOptions != null ? configurableOptions : new Configuration(),
                forStOptionsFactory,
                sharedResources,
                localBasePath,
                remoteBasePath,
                enableStatistics);
    }

    @Override
    public String toString() {
        return "ForStStateBackend{"
                + ", localForStDirectories="
                + Arrays.toString(localForStDirectories)
                + ", remoteForStDirectory="
                + remoteForStDirectory
                + '}';
    }

    // ------------------------------------------------------------------------
    //  static library loading utilities
    // ------------------------------------------------------------------------

    @VisibleForTesting
    static void ensureForStIsLoaded(String tempDirectory, Executor executor) throws IOException {
        ensureForStIsLoaded(tempDirectory, NativeLibraryLoader::getInstance, executor);
    }

    @VisibleForTesting
    static void setForStInitialized(boolean initialized) {
        forStInitialized = initialized;
    }

    @VisibleForTesting
    static void ensureForStIsLoaded(
            String tempDirectory,
            Supplier<NativeLibraryLoader> nativeLibraryLoaderSupplier,
            Executor executor)
            throws IOException {
        synchronized (ForStStateBackend.class) {
            if (!forStInitialized) {

                final File tempDirParent = new File(tempDirectory).getAbsoluteFile();
                LOG.info(
                        "Attempting to load ForSt native library and store it under '{}'",
                        tempDirParent);

                Throwable lastException = null;
                for (int attempt = 1; attempt <= FORST_LIB_LOADING_ATTEMPTS; attempt++) {
                    AtomicReference<File> rocksLibFolder = new AtomicReference<>(null);
                    try {
                        // when multiple instances of this class and ForSt exist in different
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

                        // We use an async procedure to load the library, to make current thread be
                        // able to interrupt for a fast quit.
                        CompletableFuture<Void> future =
                                FutureUtils.runAsync(
                                        () -> {
                                            File libFolder =
                                                    new File(
                                                            tempDirParent,
                                                            "rocksdb-lib-" + new AbstractID());
                                            rocksLibFolder.set(libFolder);

                                            // make sure the temp path exists
                                            LOG.debug(
                                                    "Attempting to create ForSt native library folder {}",
                                                    libFolder);
                                            // noinspection ResultOfMethodCallIgnored
                                            libFolder.mkdirs();

                                            // explicitly load the JNI dependency if it has not been
                                            // loaded before
                                            nativeLibraryLoaderSupplier
                                                    .get()
                                                    .loadLibrary(libFolder.getAbsolutePath());

                                            // this initialization here should validate that the
                                            // loading succeeded
                                            RocksDB.loadLibrary();
                                        },
                                        executor);

                        // wait for finish or be interrupted.
                        future.get();

                        // seems to have worked
                        LOG.info("Successfully loaded ForSt native library");
                        forStInitialized = true;
                        return;
                    } catch (Throwable t) {
                        lastException = t;
                        LOG.debug("ForSt JNI library loading attempt {} failed", attempt, t);

                        // try to force ForSt to attempt reloading the library
                        try {
                            resetForStLoadedFlag();
                        } catch (Throwable tt) {
                            LOG.debug(
                                    "Failed to reset 'initialized' flag in ForSt native code loader",
                                    tt);
                        }

                        FileUtils.deleteDirectoryQuietly(rocksLibFolder.get());
                    }
                }

                throw new IOException("Could not load the native ForSt library", lastException);
            }
        }
    }

    @VisibleForTesting
    static void resetForStLoadedFlag() throws Exception {
        final Field initField = NativeLibraryLoader.class.getDeclaredField("initialized");
        initField.setAccessible(true);
        initField.setBoolean(null, false);
    }

    /** The options to chose for the type of priority queue state. */
    public enum PriorityQueueStateType implements DescribedEnum {
        HEAP(text("Heap-based")),
        ForStDB(text("Implementation based on RocksDB"));

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
