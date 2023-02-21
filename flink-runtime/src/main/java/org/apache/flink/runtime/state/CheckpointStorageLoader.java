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

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.delegate.DelegatingStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.util.DynamicCodeLoadingException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.util.Optional;

/** This class contains utility methods to load checkpoint storage from configurations. */
@Internal
public class CheckpointStorageLoader {

    private static final String JOB_MANAGER_STORAGE_NAME = "jobmanager";

    private static final String FILE_SYSTEM_STORAGE_NAME = "filesystem";

    private static final String LEGACY_PRECEDENCE_LOG_MESSAGE =
            "Legacy state backends can also be used as checkpoint storage and take precedence for backward-compatibility reasons.";

    /**
     * Loads the checkpoint storage from the configuration, from the parameter
     * 'state.checkpoint-storage', as defined in {@link CheckpointingOptions#CHECKPOINT_STORAGE}.
     *
     * <p>The implementation can be specified either via their shortcut name, or via the class name
     * of a {@link CheckpointStorageFactory}. If a CheckpointStorageFactory class name is specified,
     * the factory is instantiated (via its zero-argument constructor) and its {@link
     * CheckpointStorageFactory#createFromConfig(ReadableConfig, ClassLoader)} method is called.
     *
     * <p>Recognized shortcut names are '{@value #JOB_MANAGER_STORAGE_NAME}', and '{@value
     * #FILE_SYSTEM_STORAGE_NAME}'.
     *
     * @param config The configuration to load the checkpoint storage from
     * @param classLoader The class loader that should be used to load the checkpoint storage
     * @param logger Optionally, a logger to log actions to (may be null)
     * @return The instantiated checkpoint storage.
     * @throws DynamicCodeLoadingException Thrown if a checkpoint storage factory is configured and
     *     the factory class was not found or the factory could not be instantiated
     * @throws IllegalConfigurationException May be thrown by the CheckpointStorageFactory when
     *     creating / configuring the checkpoint storage in the factory
     */
    public static Optional<CheckpointStorage> fromConfig(
            ReadableConfig config, ClassLoader classLoader, @Nullable Logger logger)
            throws IllegalStateException, DynamicCodeLoadingException {

        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(classLoader, "classLoader");

        final String storageName = config.get(CheckpointingOptions.CHECKPOINT_STORAGE);
        if (storageName == null) {
            if (logger != null) {
                logger.debug(
                        "The configuration {} has not be set in the current"
                                + " sessions flink-conf.yaml. Falling back to a default CheckpointStorage"
                                + " type. Users are strongly encouraged explicitly set this configuration"
                                + " so they understand how their applications are checkpointing"
                                + " snapshots for fault-tolerance.",
                        CheckpointingOptions.CHECKPOINT_STORAGE.key());
            }
            return Optional.empty();
        }

        switch (storageName.toLowerCase()) {
            case JOB_MANAGER_STORAGE_NAME:
                return Optional.of(createJobManagerCheckpointStorage(config, classLoader, logger));

            case FILE_SYSTEM_STORAGE_NAME:
                return Optional.of(createFileSystemCheckpointStorage(config, classLoader, logger));

            default:
                if (logger != null) {
                    logger.info("Loading state backend via factory '{}'", storageName);
                }

                CheckpointStorageFactory<?> factory;
                try {
                    @SuppressWarnings("rawtypes")
                    Class<? extends CheckpointStorageFactory> clazz =
                            Class.forName(storageName, false, classLoader)
                                    .asSubclass(CheckpointStorageFactory.class);

                    factory = clazz.newInstance();
                } catch (ClassNotFoundException e) {
                    throw new DynamicCodeLoadingException(
                            "Cannot find configured state backend factory class: " + storageName,
                            e);
                } catch (ClassCastException | InstantiationException | IllegalAccessException e) {
                    throw new DynamicCodeLoadingException(
                            "The class configured under '"
                                    + CheckpointingOptions.CHECKPOINT_STORAGE.key()
                                    + "' is not a valid checkpoint storage factory ("
                                    + storageName
                                    + ')',
                            e);
                }

                return Optional.of(factory.createFromConfig(config, classLoader));
        }
    }

    /**
     * Loads the configured {@link CheckpointStorage} for the job based on the following precedent
     * rules:
     *
     * <p>1) If the jobs configured {@link StateBackend} implements {@code CheckpointStorage} it
     * will always be used. This is to maintain backwards compatibility with older versions of Flink
     * that intermixed these responsibilities.
     *
     * <p>2) Use the {@link CheckpointStorage} instance configured via the {@code
     * StreamExecutionEnvironment}.
     *
     * <p>3) Use the {@link CheckpointStorage} instance configured via the clusters
     * <b>flink-conf.yaml</b>.
     *
     * <p>4) Load a default {@link CheckpointStorage} instance.
     *
     * @param fromApplication The checkpoint storage instance passed to the jobs
     *     StreamExecutionEnvironment. Or null if not was set.
     * @param configuredStateBackend The jobs configured state backend.
     * @param config The configuration to load the checkpoint storage from.
     * @param classLoader The class loader that should be used to load the checkpoint storage.
     * @param logger Optionally, a logger to log actions to (may be null).
     * @return The configured checkpoint storage instance.
     * @throws DynamicCodeLoadingException Thrown if a checkpoint storage factory is configured and
     *     the factory class was not found or the factory could not be instantiated
     * @throws IllegalConfigurationException May be thrown by the CheckpointStorageFactory when
     *     creating / configuring the checkpoint storage in the factory
     */
    public static CheckpointStorage load(
            @Nullable CheckpointStorage fromApplication,
            @Nullable Path defaultSavepointDirectory,
            StateBackend configuredStateBackend,
            Configuration config,
            ClassLoader classLoader,
            @Nullable Logger logger)
            throws IllegalConfigurationException, DynamicCodeLoadingException {

        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(classLoader, "classLoader");
        Preconditions.checkNotNull(configuredStateBackend, "statebackend");

        if (defaultSavepointDirectory != null) {
            // If a savepoint directory was manually specified in code
            // we override any value set in the flink-conf. This allows
            // us to pass this value to the CheckpointStorage instance
            // where it is needed at runtime while keeping its API logically
            // separated for users.
            config.set(
                    CheckpointingOptions.SAVEPOINT_DIRECTORY, defaultSavepointDirectory.toString());
        }

        // Legacy state backends always take precedence for backwards compatibility.
        StateBackend rootStateBackend =
                (configuredStateBackend instanceof DelegatingStateBackend)
                        ? ((DelegatingStateBackend) configuredStateBackend)
                                .getDelegatedStateBackend()
                        : configuredStateBackend;

        if (rootStateBackend instanceof CheckpointStorage) {
            if (logger != null) {
                logger.info(
                        "Using legacy state backend {} as Job checkpoint storage",
                        rootStateBackend);
                if (fromApplication != null) {
                    logger.warn(
                            "Checkpoint storage passed via StreamExecutionEnvironment is ignored because legacy state backend '{}' is used. {}",
                            rootStateBackend.getClass().getName(),
                            LEGACY_PRECEDENCE_LOG_MESSAGE);
                }
                if (config.get(CheckpointingOptions.CHECKPOINT_STORAGE) != null) {
                    logger.warn(
                            "Config option '{}' is ignored because legacy state backend '{}' is used. {}",
                            CheckpointingOptions.CHECKPOINT_STORAGE.key(),
                            rootStateBackend.getClass().getName(),
                            LEGACY_PRECEDENCE_LOG_MESSAGE);
                }
            }
            return (CheckpointStorage) rootStateBackend;
        }

        if (fromApplication != null) {
            if (fromApplication instanceof ConfigurableCheckpointStorage) {
                if (logger != null) {
                    logger.info(
                            "Using job/cluster config to configure application-defined checkpoint storage: {}",
                            fromApplication);
                    if (config.get(CheckpointingOptions.CHECKPOINT_STORAGE) != null) {
                        logger.warn(
                                "Config option '{}' is ignored because the checkpoint storage passed via StreamExecutionEnvironment takes precedence.",
                                CheckpointingOptions.CHECKPOINT_STORAGE.key());
                    }
                }
                return ((ConfigurableCheckpointStorage) fromApplication)
                        .configure(config, classLoader);
            }
            if (logger != null) {
                logger.info("Using application defined checkpoint storage: {}", fromApplication);
            }
            return fromApplication;
        }

        return fromConfig(config, classLoader, logger)
                .orElseGet(() -> createDefaultCheckpointStorage(config, classLoader, logger));
    }

    /**
     * Creates a default checkpoint storage instance if none was explicitly configured. For
     * backwards compatibility, the default storage will be {@link FileSystemCheckpointStorage} if a
     * checkpoint directory was configured, {@link
     * org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage} otherwise.
     *
     * @param config The configuration to load the checkpoint storage from
     * @param classLoader The class loader that should be used to load the checkpoint storage
     * @param logger Optionally, a logger to log actions to (may be null)
     * @return The instantiated checkpoint storage.
     * @throws IllegalConfigurationException May be thrown by the CheckpointStorageFactory when
     *     creating / configuring the checkpoint storage in the factory.
     */
    private static CheckpointStorage createDefaultCheckpointStorage(
            ReadableConfig config, ClassLoader classLoader, @Nullable Logger logger) {

        if (config.getOptional(CheckpointingOptions.CHECKPOINTS_DIRECTORY).isPresent()) {
            return createFileSystemCheckpointStorage(config, classLoader, logger);
        }

        return createJobManagerCheckpointStorage(config, classLoader, logger);
    }

    private static CheckpointStorage createFileSystemCheckpointStorage(
            ReadableConfig config, ClassLoader classLoader, @Nullable Logger logger) {
        FileSystemCheckpointStorage storage =
                FileSystemCheckpointStorage.createFromConfig(config, classLoader);
        if (logger != null) {
            logger.info(
                    "Checkpoint storage is set to '{}': (checkpoints \"{}\")",
                    FILE_SYSTEM_STORAGE_NAME,
                    storage.getCheckpointPath());
        }
        return storage;
    }

    private static CheckpointStorage createJobManagerCheckpointStorage(
            ReadableConfig config, ClassLoader classLoader, @Nullable Logger logger) {
        if (logger != null) {
            logger.info("Checkpoint storage is set to '{}'", JOB_MANAGER_STORAGE_NAME);
        }
        return JobManagerCheckpointStorage.createFromConfig(config, classLoader);
    }
}
