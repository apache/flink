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
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.testutils.AllCallbackWrapper;
import org.apache.flink.core.testutils.TestContainerExtension;
import org.apache.flink.fs.s3.common.MinioTestContainer;
import org.apache.flink.runtime.fs.hdfs.AbstractHadoopFileSystemITTest;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.util.UUID;

/**
 * Unit tests for the S3 file system support via Hadoop's {@link
 * org.apache.hadoop.fs.s3a.S3AFileSystem}.
 *
 * <p><strong>BEWARE</strong>: tests must take special care of S3's <a
 * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction
 * .html#ConsistencyModel">consistency guarantees</a> and what the {@link
 * org.apache.hadoop.fs.s3a.S3AFileSystem} offers.
 */
public class HadoopS3FileSystemITCase extends AbstractHadoopFileSystemITTest {
    private static final String TEST_DATA_DIR = "tests-" + UUID.randomUUID();

    @RegisterExtension
    private static final AllCallbackWrapper<TestContainerExtension<MinioTestContainer>>
            MINIO_EXTENSION =
                    new AllCallbackWrapper<>(new TestContainerExtension<>(MinioTestContainer::new));

    @BeforeAll
    static void setup() {
        // initialize configuration with valid credentials
        final Configuration conf = new Configuration();
        getMinioContainer().setS3ConfigOptions(conf);
        getMinioContainer().initializeFileSystem(conf);
        consistencyToleranceNS = 30_000_000_000L; // 30 seconds
    }

    @Override
    protected FileSystem getFileSystem() throws IOException {
        return getBasePath().getFileSystem();
    }

    @Override
    protected Path getBasePath() {
        return new Path(getMinioContainer().getS3UriForDefaultBucket() + "/temp/" + TEST_DATA_DIR);
    }

    private static MinioTestContainer getMinioContainer() {
        return MINIO_EXTENSION.getCustomExtension().getTestContainer();
    }
}
