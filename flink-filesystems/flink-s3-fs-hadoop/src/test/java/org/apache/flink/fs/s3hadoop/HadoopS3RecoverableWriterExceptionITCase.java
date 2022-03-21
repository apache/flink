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
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.fs.s3.common.FlinkS3FileSystem;
import org.apache.flink.runtime.fs.hdfs.AbstractHadoopRecoverableWriterExceptionITCase;
import org.apache.flink.testutils.s3.S3TestCredentials;

import org.junit.BeforeClass;

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

    @BeforeClass
    public static void checkCredentialsAndSetup() throws IOException {
        // check whether credentials exist
        S3TestCredentials.assumeCredentialsAvailable();

        basePath = new Path(S3TestCredentials.getTestBucketUri() + "tests-" + UUID.randomUUID());

        // initialize configuration with valid credentials
        final Configuration conf = new Configuration();
        conf.setString("s3.access.key", S3TestCredentials.getS3AccessKey());
        conf.setString("s3.secret.key", S3TestCredentials.getS3SecretKey());

        conf.setLong(PART_UPLOAD_MIN_SIZE, PART_UPLOAD_MIN_SIZE_VALUE);
        conf.setInteger(MAX_CONCURRENT_UPLOADS, MAX_CONCURRENT_UPLOADS_VALUE);

        final String defaultTmpDir = TEMP_FOLDER.getRoot().getAbsolutePath() + "s3_tmp_dir";
        conf.setString(CoreOptions.TMP_DIRS, defaultTmpDir);

        FileSystem.initialize(conf);

        skipped = false;
    }

    @Override
    protected String getLocalTmpDir() throws Exception {
        return ((FlinkS3FileSystem) getFileSystem()).getLocalTmpDir();
    }
}
