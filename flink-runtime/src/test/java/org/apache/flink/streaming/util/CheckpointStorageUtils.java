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

package org.apache.flink.streaming.util;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Preconditions;

import java.net.URI;
import java.nio.file.Path;

/** A utility class for configuring checkpoint storage. */
public class CheckpointStorageUtils {

    /**
     * Configures the checkpoint storage to use the JobManager for storing checkpoints.
     *
     * @param env The StreamExecutionEnvironment to configure.
     */
    public static void configureJobManagerCheckpointStorage(StreamExecutionEnvironment env) {
        env.configure(
                new Configuration().set(CheckpointingOptions.CHECKPOINT_STORAGE, "jobmanager"));
    }

    /**
     * Configures the checkpoint storage with a given directory as a string.
     *
     * @param env The StreamExecutionEnvironment to configure.
     * @param checkpointDirectory The directory where checkpoints will be stored, must not be null.
     * @throws NullPointerException if checkpointDirectory is null.
     */
    public static void configureFileSystemCheckpointStorage(
            StreamExecutionEnvironment env, String checkpointDirectory) {
        Preconditions.checkNotNull(checkpointDirectory, "Checkpoint directory must not be null");
        env.configure(
                new Configuration()
                        .set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDirectory));
    }

    public static void configureFileSystemCheckpointStorage(
            StreamExecutionEnvironment env, String checkpointDirectory, long stateThreshold) {
        Preconditions.checkNotNull(checkpointDirectory, "Checkpoint directory must not be null");
        configureFileSystemCheckpointStorage(env, checkpointDirectory);

        MemorySize memorySize = MemorySize.parse(stateThreshold + "b");
        env.configure(
                new Configuration().set(CheckpointingOptions.FS_SMALL_FILE_THRESHOLD, memorySize));
    }

    /**
     * Configures the checkpoint storage with a given directory as a URI.
     *
     * @param env The StreamExecutionEnvironment to configure.
     * @param checkpointDirectory The URI of the directory where checkpoints will be stored, must
     *     not be null.
     * @throws NullPointerException if checkpointDirectory is null.
     */
    public static void configureFileSystemCheckpointStorage(
            StreamExecutionEnvironment env, URI checkpointDirectory) {
        Preconditions.checkNotNull(checkpointDirectory, "Checkpoint directory must not be null");
        configureFileSystemCheckpointStorage(env, checkpointDirectory.toString());
    }

    /**
     * Sets the checkpoint storage with a given directory as a Path.
     *
     * @param env The StreamExecutionEnvironment to configure.
     * @param checkpointDirectory The Path of the directory where checkpoints will be stored, must
     *     not be null.
     * @throws NullPointerException if checkpointDirectory is null.
     */
    public static void configureFileSystemCheckpointStorage(
            StreamExecutionEnvironment env, Path checkpointDirectory) {
        Preconditions.checkNotNull(checkpointDirectory, "Checkpoint directory must not be null");
        configureFileSystemCheckpointStorage(env, checkpointDirectory.toString());
    }

    /**
     * Configures the checkpoint storage using a specified storage factory.
     *
     * @param env The StreamExecutionEnvironment to configure.
     * @param checkpointStorageFactory The fully qualified class name of the checkpoint storage
     *     factory, must not be null or empty.
     * @throws IllegalArgumentException if checkpointStorageFactory is null or empty.
     */
    public static void configureCheckpointStorageWithFactory(
            StreamExecutionEnvironment env, String checkpointStorageFactory) {
        Preconditions.checkNotNull(
                checkpointStorageFactory != null, "Checkpoint storage factory must not be null.");

        env.configure(
                new Configuration()
                        .set(CheckpointingOptions.CHECKPOINT_STORAGE, checkpointStorageFactory));
    }
}
