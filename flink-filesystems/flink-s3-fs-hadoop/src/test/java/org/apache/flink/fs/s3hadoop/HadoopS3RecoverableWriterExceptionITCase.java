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

package org.apache.flink.fs.s3hadoop;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.testutils.AllCallbackWrapper;
import org.apache.flink.core.testutils.TestContainerExtension;
import org.apache.flink.fs.s3.common.FlinkS3FileSystem;
import org.apache.flink.fs.s3.common.MinioTestContainer;
import org.apache.flink.runtime.fs.hdfs.AbstractHadoopRecoverableWriterExceptionITCase;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.util.UUID;

import static org.apache.flink.fs.s3.common.AbstractS3FileSystemFactory.MAX_CONCURRENT_UPLOADS;
import static org.apache.flink.fs.s3.common.AbstractS3FileSystemFactory.PART_UPLOAD_MIN_SIZE;

/**
 * Tests for exception throwing in the {@link
 * org.apache.flink.fs.s3.common.writer.S3RecoverableWriter S3RecoverableWriter}.
 */
public class HadoopS3RecoverableWriterExceptionITCase
        extends AbstractHadoopRecoverableWriterExceptionITCase {

    // ----------------------- S3 general configuration -----------------------

    private static final long PART_UPLOAD_MIN_SIZE_VALUE = 7L << 20;
    private static final int MAX_CONCURRENT_UPLOADS_VALUE = 2;

    @RegisterExtension
    private static final AllCallbackWrapper<TestContainerExtension<MinioTestContainer>>
            MINIO_EXTENSION =
                    new AllCallbackWrapper<>(new TestContainerExtension<>(MinioTestContainer::new));

    @BeforeAll
    public static void checkCredentialsAndSetup() throws IOException {
        // initialize configuration with valid credentials
        final Configuration conf = new Configuration();
        MINIO_EXTENSION.getCustomExtension().getTestContainer().setS3ConfigOptions(conf);

        conf.set(PART_UPLOAD_MIN_SIZE, PART_UPLOAD_MIN_SIZE_VALUE);
        conf.set(MAX_CONCURRENT_UPLOADS, MAX_CONCURRENT_UPLOADS_VALUE);

        final String defaultTmpDir = tempFolder.getAbsolutePath() + "s3_tmp_dir";
        conf.set(CoreOptions.TMP_DIRS, defaultTmpDir);

        MINIO_EXTENSION.getCustomExtension().getTestContainer().initializeFileSystem(conf);

        skipped = false;
    }

    @Override
    protected String getLocalTmpDir() throws Exception {
        return ((FlinkS3FileSystem) getFileSystem()).getLocalTmpDir();
    }

    @Override
    protected Path getBasePath() {
        return new Path(
                MINIO_EXTENSION.getCustomExtension().getTestContainer().getS3UriForDefaultBucket()
                        + "/temp/tests-"
                        + UUID.randomUUID());
    }
}
