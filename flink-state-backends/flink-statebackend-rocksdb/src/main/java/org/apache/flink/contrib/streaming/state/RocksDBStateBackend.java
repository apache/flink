/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractManagedMemoryStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageAccess;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.ConfigurableStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.TernaryBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * <b>IMPORTANT</b> {@link RocksDBStateBackend} is deprecated in favor of {@link
 * EmbeddedRocksDBStateBackend}. and {@link
 * org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage}. This change does not affect
 * the runtime characteristics of your Jobs and is simply an API change to help better communicate
 * the ways Flink separates local state storage from fault tolerance. Jobs can be upgraded without
 * loss of state. If configuring your state backend via the {@code StreamExecutionEnvironment}
 * please make the following changes.
 *
 * <pre>{@code
 * 		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
 * 		env.setStateBackend(new EmbeddedRocksDBStateBackend());
 * 		env.getCheckpointConfig().setCheckpointStorage("hdfs://checkpoints");
 * }</pre>
 *
 * <p>If you are configuring your state backend via the {@code flink-conf.yaml} no changes are
 * required.
 *
 * <p>A State Backend that stores its state in {@code RocksDB}. This state backend can store very
 * large state that exceeds memory and spills to disk.
 *
 * <p>All key/value state (including windows) is stored in the key/value index of RocksDB. For
 * persistence against loss of machines, checkpoints take a snapshot of the RocksDB database, and
 * persist that snapshot in a file system (by default) or another configurable state backend.
 *
 * <p>The behavior of the RocksDB instances can be parametrized by setting RocksDB Options using the
 * methods {@link #setPredefinedOptions(PredefinedOptions)} and {@link
 * #setRocksDBOptions(RocksDBOptionsFactory)}.
 */
@Deprecated
public class RocksDBStateBackend extends AbstractManagedMemoryStateBackend
        implements CheckpointStorage, ConfigurableStateBackend {

    /** The options to chose for the type of priority queue state. */
    @Deprecated
    public enum PriorityQueueStateType {
        HEAP,
        ROCKSDB
    }

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBStateBackend.class);

    // ------------------------------------------------------------------------

    // -- configuration values, set in the application / configuration

    private final EmbeddedRocksDBStateBackend rocksDBStateBackend;

    /** The checkpoint storage that we use for creating checkpoint streams. */
    private final StateBackend checkpointStreamBackend;

    // ------------------------------------------------------------------------

    /**
     * Creates a new {@code RocksDBStateBackend} that stores its checkpoint data in the file system
     * and location defined by the given URI.
     *
     * <p>A state backend that stores checkpoints in HDFS or S3 must specify the file system host
     * and port in the URI, or have the Hadoop configuration that describes the file system (host /
     * high-availability group / possibly credentials) either referenced from the Flink config, or
     * included in the classpath.
     *
     * @param checkpointDataUri The URI describing the filesystem and path to the checkpoint data
     *     directory.
     * @throws IOException Thrown, if no file system can be found for the scheme in the URI.
     */
    public RocksDBStateBackend(String checkpointDataUri) throws IOException {
        this(new Path(checkpointDataUri).toUri());
    }

    /**
     * Creates a new {@code RocksDBStateBackend} that stores its checkpoint data in the file system
     * and location defined by the given URI.
     *
     * <p>A state backend that stores checkpoints in HDFS or S3 must specify the file system host
     * and port in the URI, or have the Hadoop configuration that describes the file system (host /
     * high-availability group / possibly credentials) either referenced from the Flink config, or
     * included in the classpath.
     *
     * @param checkpointDataUri The URI describing the filesystem and path to the checkpoint data
     *     directory.
     * @param enableIncrementalCheckpointing True if incremental checkpointing is enabled.
     * @throws IOException Thrown, if no file system can be found for the scheme in the URI.
     */
    public RocksDBStateBackend(String checkpointDataUri, boolean enableIncrementalCheckpointing)
            throws IOException {
        this(new Path(checkpointDataUri).toUri(), enableIncrementalCheckpointing);
    }

    /**
     * Creates a new {@code RocksDBStateBackend} that stores its checkpoint data in the file system
     * and location defined by the given URI.
     *
     * <p>A state backend that stores checkpoints in HDFS or S3 must specify the file system host
     * and port in the URI, or have the Hadoop configuration that describes the file system (host /
     * high-availability group / possibly credentials) either referenced from the Flink config, or
     * included in the classpath.
     *
     * @param checkpointDataUri The URI describing the filesystem and path to the checkpoint data
     *     directory.
     * @throws IOException Thrown, if no file system can be found for the scheme in the URI.
     */
    public RocksDBStateBackend(URI checkpointDataUri) throws IOException {
        this(new FsStateBackend(checkpointDataUri));
    }

    /**
     * Creates a new {@code RocksDBStateBackend} that stores its checkpoint data in the file system
     * and location defined by the given URI.
     *
     * <p>A state backend that stores checkpoints in HDFS or S3 must specify the file system host
     * and port in the URI, or have the Hadoop configuration that describes the file system (host /
     * high-availability group / possibly credentials) either referenced from the Flink config, or
     * included in the classpath.
     *
     * @param checkpointDataUri The URI describing the filesystem and path to the checkpoint data
     *     directory.
     * @param enableIncrementalCheckpointing True if incremental checkpointing is enabled.
     * @throws IOException Thrown, if no file system can be found for the scheme in the URI.
     */
    public RocksDBStateBackend(URI checkpointDataUri, boolean enableIncrementalCheckpointing)
            throws IOException {
        this(new FsStateBackend(checkpointDataUri), enableIncrementalCheckpointing);
    }

    /**
     * Creates a new {@code RocksDBStateBackend} that uses the given state backend to store its
     * checkpoint data streams. Typically, one would supply a filesystem or database state backend
     * here where the snapshots from RocksDB would be stored.
     *
     * <p>The snapshots of the RocksDB state will be stored using the given backend's {@link
     * CheckpointStorage#createCheckpointStorage(JobID)}.
     *
     * @param checkpointStreamBackend The backend write the checkpoint streams to.
     */
    public RocksDBStateBackend(StateBackend checkpointStreamBackend) {
        this(checkpointStreamBackend, TernaryBoolean.UNDEFINED);
    }

    /**
     * Creates a new {@code RocksDBStateBackend} that uses the given state backend to store its
     * checkpoint data streams. Typically, one would supply a filesystem or database state backend
     * here where the snapshots from RocksDB would be stored.
     *
     * <p>The snapshots of the RocksDB state will be stored using the given backend's {@code
     * StateBackend#createCheckpointStorage(JobID)}.
     *
     * @param checkpointStreamBackend The backend write the checkpoint streams to.
     * @param enableIncrementalCheckpointing True if incremental checkpointing is enabled.
     */
    public RocksDBStateBackend(
            StateBackend checkpointStreamBackend, TernaryBoolean enableIncrementalCheckpointing) {
        if (!(checkpointStreamBackend instanceof CheckpointStorage)) {
            throw new IllegalStateException(
                    "RocksDBStateBackend can only checkpoint"
                            + "to state backends that also implement CheckpointStorage.");
        }
        this.checkpointStreamBackend = checkNotNull(checkpointStreamBackend);
        this.rocksDBStateBackend = new EmbeddedRocksDBStateBackend(enableIncrementalCheckpointing);
    }

    /** @deprecated Use {@link #RocksDBStateBackend(StateBackend)} instead. */
    @Deprecated
    public RocksDBStateBackend(AbstractStateBackend checkpointStreamBackend) {
        this(checkpointStreamBackend, TernaryBoolean.UNDEFINED);
    }

    /** @deprecated Use {@link #RocksDBStateBackend(StateBackend, TernaryBoolean)} instead. */
    @Deprecated
    public RocksDBStateBackend(
            AbstractStateBackend checkpointStreamBackend, boolean enableIncrementalCheckpointing) {
        this(checkpointStreamBackend, TernaryBoolean.fromBoolean(enableIncrementalCheckpointing));
    }

    /**
     * Private constructor that creates a re-configured copy of the state backend.
     *
     * @param original The state backend to re-configure.
     * @param config The configuration.
     * @param classLoader The class loader.
     */
    private RocksDBStateBackend(
            RocksDBStateBackend original, ReadableConfig config, ClassLoader classLoader) {
        // reconfigure the state backend backing the streams
        final StateBackend originalStreamBackend = original.checkpointStreamBackend;
        this.checkpointStreamBackend =
                originalStreamBackend instanceof ConfigurableStateBackend
                        ? ((ConfigurableStateBackend) originalStreamBackend)
                                .configure(config, classLoader)
                        : originalStreamBackend;
        this.rocksDBStateBackend = original.rocksDBStateBackend.configure(config, classLoader);
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
    public RocksDBStateBackend configure(ReadableConfig config, ClassLoader classLoader) {
        return new RocksDBStateBackend(this, config, classLoader);
    }

    // ------------------------------------------------------------------------
    //  State backend methods
    // ------------------------------------------------------------------------

    /**
     * Gets the state backend that this RocksDB state backend uses to persist its bytes to.
     *
     * <p>This RocksDB state backend only implements the RocksDB specific parts, it relies on the
     * 'CheckpointBackend' to persist the checkpoint and savepoint bytes streams.
     */
    public StateBackend getCheckpointBackend() {
        return checkpointStreamBackend;
    }

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

    // ------------------------------------------------------------------------
    //  Checkpoint initialization and persistent storage
    // ------------------------------------------------------------------------

    @Override
    public CompletedCheckpointStorageLocation resolveCheckpoint(String pointer) throws IOException {
        return ((CheckpointStorage) checkpointStreamBackend).resolveCheckpoint(pointer);
    }

    @Override
    public CheckpointStorageAccess createCheckpointStorage(JobID jobId) throws IOException {
        return ((CheckpointStorage) checkpointStreamBackend).createCheckpointStorage(jobId);
    }

    // ------------------------------------------------------------------------
    //  State holding data structures
    // ------------------------------------------------------------------------

    @Override
    public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
            Environment env,
            JobID jobID,
            String operatorIdentifier,
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            TaskKvStateRegistry kvStateRegistry,
            TtlTimeProvider ttlTimeProvider,
            MetricGroup metricGroup,
            @Nonnull Collection<KeyedStateHandle> stateHandles,
            CloseableRegistry cancelStreamRegistry)
            throws IOException {
        return rocksDBStateBackend.createKeyedStateBackend(
                env,
                jobID,
                operatorIdentifier,
                keySerializer,
                numberOfKeyGroups,
                keyGroupRange,
                kvStateRegistry,
                ttlTimeProvider,
                metricGroup,
                stateHandles,
                cancelStreamRegistry);
    }

    @Override
    public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
            Environment env,
            JobID jobID,
            String operatorIdentifier,
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            TaskKvStateRegistry kvStateRegistry,
            TtlTimeProvider ttlTimeProvider,
            MetricGroup metricGroup,
            @Nonnull Collection<KeyedStateHandle> stateHandles,
            CloseableRegistry cancelStreamRegistry,
            double managedMemoryFraction)
            throws IOException {
        return rocksDBStateBackend.createKeyedStateBackend(
                env,
                jobID,
                operatorIdentifier,
                keySerializer,
                numberOfKeyGroups,
                keyGroupRange,
                kvStateRegistry,
                ttlTimeProvider,
                metricGroup,
                stateHandles,
                cancelStreamRegistry,
                managedMemoryFraction);
    }

    @Override
    public OperatorStateBackend createOperatorStateBackend(
            Environment env,
            String operatorIdentifier,
            @Nonnull Collection<OperatorStateHandle> stateHandles,
            CloseableRegistry cancelStreamRegistry)
            throws Exception {
        return rocksDBStateBackend.createOperatorStateBackend(
                env, operatorIdentifier, stateHandles, cancelStreamRegistry);
    }

    // ------------------------------------------------------------------------
    //  Parameters
    // ------------------------------------------------------------------------

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
        rocksDBStateBackend.setDbStoragePaths(paths);
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
        return rocksDBStateBackend.getDbStoragePaths();
    }

    /** Gets whether incremental checkpoints are enabled for this state backend. */
    public boolean isIncrementalCheckpointsEnabled() {
        return rocksDBStateBackend.isIncrementalCheckpointsEnabled();
    }

    /**
     * Gets the type of the priority queue state. It will fallback to the default value, if it is
     * not explicitly set.
     *
     * @return The type of the priority queue state.
     */
    public PriorityQueueStateType getPriorityQueueStateType() {
        return LegacyEnumBridge.convert(rocksDBStateBackend.getPriorityQueueStateType());
    }

    /**
     * Sets the type of the priority queue state. It will fallback to the default value, if it is
     * not explicitly set.
     */
    public void setPriorityQueueStateType(PriorityQueueStateType priorityQueueStateType) {
        rocksDBStateBackend.setPriorityQueueStateType(
                LegacyEnumBridge.convert(priorityQueueStateType));
    }

    // ------------------------------------------------------------------------
    //  Parametrize with RocksDB Options
    // ------------------------------------------------------------------------

    /**
     * Sets the predefined options for RocksDB.
     *
     * <p>If user-configured options within {@link RocksDBConfigurableOptions} is set (through
     * flink-conf.yaml) or a user-defined options factory is set (via {@link
     * #setRocksDBOptions(RocksDBOptionsFactory)}), then the options from the factory are applied on
     * top of the here specified predefined options and customized options.
     *
     * @param options The options to set (must not be null).
     */
    public void setPredefinedOptions(@Nonnull PredefinedOptions options) {
        rocksDBStateBackend.setPredefinedOptions(options);
    }

    /**
     * Gets the currently set predefined options for RocksDB. The default options (if nothing was
     * set via {@link #setPredefinedOptions(PredefinedOptions)}) are {@link
     * PredefinedOptions#DEFAULT}.
     *
     * <p>If user-configured options within {@link RocksDBConfigurableOptions} is set (through
     * flink-conf.yaml) of a user-defined options factory is set (via {@link
     * #setRocksDBOptions(RocksDBOptionsFactory)}), then the options from the factory are applied on
     * top of the predefined and customized options.
     *
     * @return The currently set predefined options for RocksDB.
     */
    @VisibleForTesting
    public PredefinedOptions getPredefinedOptions() {
        return rocksDBStateBackend.getPredefinedOptions();
    }

    /** @return The underlying {@link EmbeddedRocksDBStateBackend} instance. */
    @VisibleForTesting
    EmbeddedRocksDBStateBackend getEmbeddedRocksDBStateBackend() {
        return rocksDBStateBackend;
    }

    /**
     * Sets {@link org.rocksdb.Options} for the RocksDB instances. Because the options are not
     * serializable and hold native code references, they must be specified through a factory.
     *
     * <p>The options created by the factory here are applied on top of the pre-defined options
     * profile selected via {@link #setPredefinedOptions(PredefinedOptions)}. If the pre-defined
     * options profile is the default ({@link PredefinedOptions#DEFAULT}), then the factory fully
     * controls the RocksDB options.
     *
     * @param optionsFactory The options factory that lazily creates the RocksDB options.
     */
    public void setRocksDBOptions(RocksDBOptionsFactory optionsFactory) {
        rocksDBStateBackend.setRocksDBOptions(optionsFactory);
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
        return rocksDBStateBackend.getRocksDBOptions();
    }

    /** Gets the number of threads used to transfer files while snapshotting/restoring. */
    public int getNumberOfTransferThreads() {
        return rocksDBStateBackend.getNumberOfTransferThreads();
    }

    /**
     * Sets the number of threads used to transfer files while snapshotting/restoring.
     *
     * @param numberOfTransferThreads The number of threads used to transfer files while
     *     snapshotting/restoring.
     */
    public void setNumberOfTransferThreads(int numberOfTransferThreads) {
        rocksDBStateBackend.setNumberOfTransferThreads(numberOfTransferThreads);
    }

    /** @deprecated Typo in method name. Use {@link #getNumberOfTransferThreads} instead. */
    @Deprecated
    public int getNumberOfTransferingThreads() {
        return getNumberOfTransferThreads();
    }

    /** @deprecated Typo in method name. Use {@link #setNumberOfTransferThreads(int)} instead. */
    @Deprecated
    public void setNumberOfTransferingThreads(int numberOfTransferingThreads) {
        setNumberOfTransferThreads(numberOfTransferingThreads);
    }

    /** Gets the max batch size will be used in {@link RocksDBWriteBatchWrapper}. */
    public long getWriteBatchSize() {
        return rocksDBStateBackend.getWriteBatchSize();
    }

    /**
     * Sets the max batch size will be used in {@link RocksDBWriteBatchWrapper}, no positive value
     * will disable memory size controller, just use item count controller.
     *
     * @param writeBatchSize The size will used to be used in {@link RocksDBWriteBatchWrapper}.
     */
    public void setWriteBatchSize(long writeBatchSize) {
        rocksDBStateBackend.setWriteBatchSize(writeBatchSize);
    }

    // ------------------------------------------------------------------------
    //  utilities
    // ------------------------------------------------------------------------

    @VisibleForTesting
    RocksDBResourceContainer createOptionsAndResourceContainer() {
        return rocksDBStateBackend.createOptionsAndResourceContainer(null);
    }

    @Override
    public String toString() {
        return "RocksDBStateBackend{"
                + "checkpointStreamBackend="
                + checkpointStreamBackend
                + ", localRocksDbDirectories="
                + Arrays.toString(rocksDBStateBackend.getDbStoragePaths())
                + ", enableIncrementalCheckpointing="
                + rocksDBStateBackend.isIncrementalCheckpointsEnabled()
                + ", numberOfTransferThreads="
                + rocksDBStateBackend.getNumberOfTransferThreads()
                + ", writeBatchSize="
                + rocksDBStateBackend.getWriteBatchSize()
                + '}';
    }

    // ------------------------------------------------------------------------
    //  static library loading utilities
    // ------------------------------------------------------------------------

    @VisibleForTesting
    static void ensureRocksDBIsLoaded(String tempDirectory) throws IOException {
        EmbeddedRocksDBStateBackend.ensureRocksDBIsLoaded(tempDirectory);
    }

    @VisibleForTesting
    static void resetRocksDBLoadedFlag() throws Exception {
        EmbeddedRocksDBStateBackend.resetRocksDBLoadedFlag();
    }
}
