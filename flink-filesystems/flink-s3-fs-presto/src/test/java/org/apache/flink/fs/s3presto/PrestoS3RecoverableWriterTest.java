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
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.testutils.AllCallbackWrapper;
import org.apache.flink.core.testutils.TestContainerExtension;
import org.apache.flink.fs.s3.common.FlinkS3FileSystem;
import org.apache.flink.fs.s3.common.MinioTestContainer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;

import static org.apache.flink.fs.s3.common.AbstractS3FileSystemFactory.MAX_CONCURRENT_UPLOADS;
import static org.apache.flink.fs.s3.common.AbstractS3FileSystemFactory.PART_UPLOAD_MIN_SIZE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link org.apache.flink.core.fs.RecoverableWriter} of the Presto S3 FS. */
public class PrestoS3RecoverableWriterTest {

    // ----------------------- S3 general configuration -----------------------

    private static final long PART_UPLOAD_MIN_SIZE_VALUE = 7L << 20;
    private static final int MAX_CONCURRENT_UPLOADS_VALUE = 2;

    // ----------------------- Test Specific configuration -----------------------

    @RegisterExtension
    private static final AllCallbackWrapper<TestContainerExtension<MinioTestContainer>>
            MINIO_EXTENSION =
                    new AllCallbackWrapper<>(new TestContainerExtension<>(MinioTestContainer::new));

    // ----------------------- Test Lifecycle -----------------------

    @BeforeAll
    public static void checkCredentialsAndSetup() {
        // check whether credentials exist

        // initialize configuration with valid credentials
        final Configuration conf = new Configuration();
        MINIO_EXTENSION.getCustomExtension().getTestContainer().setS3ConfigOptions(conf);

        conf.set(PART_UPLOAD_MIN_SIZE, PART_UPLOAD_MIN_SIZE_VALUE);
        conf.set(MAX_CONCURRENT_UPLOADS, MAX_CONCURRENT_UPLOADS_VALUE);

        final String defaultTmpDir = conf.get(CoreOptions.TMP_DIRS) + "s3_tmp_dir";
        conf.set(CoreOptions.TMP_DIRS, defaultTmpDir);

        MINIO_EXTENSION.getCustomExtension().getTestContainer().initializeFileSystem(conf);
    }

    @AfterAll
    public static void cleanUp() {
        FileSystem.initialize(new Configuration());
    }

    // ----------------------- Tests -----------------------

    @Test
    void requestingRecoverableWriterShouldThroughException() throws Exception {
        URI s3Uri =
                URI.create(
                        MINIO_EXTENSION
                                .getCustomExtension()
                                .getTestContainer()
                                .getS3UriForDefaultBucket());
        FlinkS3FileSystem fileSystem = (FlinkS3FileSystem) FileSystem.get(s3Uri);
        assertThatThrownBy(fileSystem::createRecoverableWriter)
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
