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
import org.apache.flink.fs.s3.common.AbstractS3FileSystemFactory;
import org.apache.flink.fs.s3.common.FlinkS3FileSystem;

import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test to verify that Presto S3 filesystem correctly throws UnsupportedOperationException when
 * trying to create a recoverable writer (since it doesn't have S3AccessHelper).
 *
 * <p>This test confirms that the flink-s3-fs-base updates for AWS SDK v2 don't break Presto, which
 * still uses AWS SDK v1 and doesn't participate in the S3AccessHelper abstraction.
 */
class PrestoRecoverableWriterTest {

    @Test
    void testRecoverableWriterThrowsException() throws Exception {
        final Configuration conf = new Configuration();
        conf.set(AbstractS3FileSystemFactory.ACCESS_KEY, "test_access_key_id");
        conf.set(AbstractS3FileSystemFactory.SECRET_KEY, "test_secret_access_key");

        FileSystem.initialize(conf);
        FileSystem fs = FileSystem.get(new URI("s3://test"));

        // Verify it's a Presto filesystem
        assertThat(fs).isInstanceOf(FlinkS3FileSystem.class);

        FlinkS3FileSystem s3fs = (FlinkS3FileSystem) fs;

        // This should throw UnsupportedOperationException because Presto
        // returns null for S3AccessHelper
        assertThatThrownBy(() -> s3fs.createRecoverableWriter())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("does not support recoverable writers");
    }
}
