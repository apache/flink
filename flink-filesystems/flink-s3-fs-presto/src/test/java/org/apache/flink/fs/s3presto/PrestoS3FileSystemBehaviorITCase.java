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

package org.apache.flink.fs.s3presto;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemBehaviorTestSuite;
import org.apache.flink.core.fs.FileSystemKind;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.testutils.AllCallbackWrapper;
import org.apache.flink.core.testutils.TestContainerExtension;
import org.apache.flink.fs.s3.common.MinioTestContainer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.UUID;

/**
 * An implementation of the {@link FileSystemBehaviorTestSuite} for the s3a-based S3 file system.
 */
class PrestoS3FileSystemBehaviorITCase extends FileSystemBehaviorTestSuite {

    private static final String TEST_DATA_DIR = "tests-" + UUID.randomUUID();

    @RegisterExtension
    private static final AllCallbackWrapper<TestContainerExtension<MinioTestContainer>>
            MINIO_EXTENSION =
                    new AllCallbackWrapper<>(new TestContainerExtension<>(MinioTestContainer::new));

    @BeforeAll
    static void checkCredentialsAndSetup() {
        // initialize configuration with valid credentials
        final Configuration conf = new Configuration();
        MINIO_EXTENSION.getCustomExtension().getTestContainer().setS3ConfigOptions(conf);
        MINIO_EXTENSION.getCustomExtension().getTestContainer().initializeFileSystem(conf);
    }

    @AfterAll
    static void clearFsConfig() {
        FileSystem.initialize(new Configuration(), null);
    }

    @Override
    protected FileSystem getFileSystem() throws Exception {
        return getBasePath().getFileSystem();
    }

    @Override
    protected Path getBasePath() {
        return new Path(
                MINIO_EXTENSION.getCustomExtension().getTestContainer().getS3UriForDefaultBucket()
                        + "/temp/"
                        + TEST_DATA_DIR);
    }

    @Override
    protected FileSystemKind getFileSystemKind() {
        return FileSystemKind.OBJECT_STORE;
    }
}
