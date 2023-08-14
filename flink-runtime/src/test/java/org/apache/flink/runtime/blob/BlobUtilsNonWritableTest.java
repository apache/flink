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

package org.apache.flink.runtime.blob;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.OperatingSystem;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

/** Tests for {@link BlobUtils} working on non-writable directories. */
class BlobUtilsNonWritableTest {

    private static final String CANNOT_CREATE_THIS = "cannot-create-this";

    @TempDir private Path tempDir;

    @BeforeEach
    void before() {
        assumeThat(OperatingSystem.isWindows()).as("setWritable doesn't work on Windows").isFalse();

        // Prepare test directory
        assertThat(tempDir.toFile().setExecutable(true, false)).isTrue();
        assertThat(tempDir.toFile().setReadable(true, false)).isTrue();
        assertThat(tempDir.toFile().setWritable(false, false)).isTrue();
    }

    @Test
    void testExceptionOnCreateStorageDirectoryFailure() {
        Configuration config = new Configuration();
        config.setString(
                BlobServerOptions.STORAGE_DIRECTORY, getStorageLocationFile().getAbsolutePath());

        assertThatThrownBy(() -> BlobUtils.createBlobStorageDirectory(config, null))
                .isInstanceOf(IOException.class);
    }

    @Test
    void testExceptionOnCreateCacheDirectoryFailureNoJob() {
        assertThatThrownBy(
                        () ->
                                BlobUtils.getStorageLocation(
                                        getStorageLocationFile(), null, new TransientBlobKey()))
                .isInstanceOf(IOException.class);
    }

    @Test
    void testExceptionOnCreateCacheDirectoryFailureForJobTransient() {
        assertThatThrownBy(
                        () ->
                                BlobUtils.getStorageLocation(
                                        getStorageLocationFile(),
                                        new JobID(),
                                        new TransientBlobKey()))
                .isInstanceOf(IOException.class);
    }

    @Test
    void testExceptionOnCreateCacheDirectoryFailureForJobPermanent() {
        assertThatThrownBy(
                        () ->
                                BlobUtils.getStorageLocation(
                                        getStorageLocationFile(),
                                        new JobID(),
                                        new PermanentBlobKey()))
                .isInstanceOf(IOException.class);
    }

    private File getStorageLocationFile() {
        return tempDir.resolve(CANNOT_CREATE_THIS).toFile();
    }
}
