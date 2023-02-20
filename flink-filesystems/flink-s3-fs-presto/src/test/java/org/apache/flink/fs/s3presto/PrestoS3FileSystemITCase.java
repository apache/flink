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
import org.apache.flink.core.fs.Path;
import org.apache.flink.fs.s3.common.AbstractS3FileSystemFactory;
import org.apache.flink.runtime.fs.hdfs.AbstractHadoopFileSystemITTest;
import org.apache.flink.testutils.s3.S3TestCredentials;

import com.amazonaws.SdkClientException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.UUID;

import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_USE_INSTANCE_CREDENTIALS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for the S3 file system support via Presto's {@link
 * com.facebook.presto.hive.s3.PrestoS3FileSystem}.
 *
 * <p><strong>BEWARE</strong>: tests must take special care of S3's <a
 * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html#ConsistencyModel">consistency
 * guarantees</a> and what the {@link com.facebook.presto.hive.s3.PrestoS3FileSystem} offers.
 */
class PrestoS3FileSystemITCase extends AbstractHadoopFileSystemITTest {

    private static final String TEST_DATA_DIR = "tests-" + UUID.randomUUID();

    @BeforeAll
    static void setup() throws IOException {
        S3TestCredentials.assumeCredentialsAvailable();
        // initialize configuration with valid credentials
        final Configuration conf = new Configuration();
        conf.setString("s3.access.key", S3TestCredentials.getS3AccessKey());
        conf.setString("s3.secret.key", S3TestCredentials.getS3SecretKey());
        FileSystem.initialize(conf);

        basePath = new Path(S3TestCredentials.getTestBucketUri() + TEST_DATA_DIR);
        fs = basePath.getFileSystem();
        consistencyToleranceNS = 30_000_000_000L; // 30 seconds

        // check for uniqueness of the test directory
        // directory must not yet exist
        assertThat(fs.exists(basePath)).isFalse();
    }

    @Test
    void testConfigKeysForwarding() throws Exception {
        final Path path = basePath;

        // access without credentials should fail
        {
            Configuration conf = new Configuration();
            // fail fast and do not fall back to trying EC2 credentials
            conf.setString(S3_USE_INSTANCE_CREDENTIALS, "false");
            FileSystem.initialize(conf);

            assertThatThrownBy(() -> path.getFileSystem().exists(path))
                    .isInstanceOf(SdkClientException.class);
        }

        // standard Presto-style credential keys
        {
            Configuration conf = new Configuration();
            conf.setString(S3_USE_INSTANCE_CREDENTIALS, "false");
            conf.setString("presto.s3.access-key", S3TestCredentials.getS3AccessKey());
            conf.setString("presto.s3.secret-key", S3TestCredentials.getS3SecretKey());

            FileSystem.initialize(conf);
            path.getFileSystem().exists(path);
        }

        // shortened Presto-style credential keys
        {
            Configuration conf = new Configuration();
            conf.setString(S3_USE_INSTANCE_CREDENTIALS, "false");
            conf.set(AbstractS3FileSystemFactory.ACCESS_KEY, S3TestCredentials.getS3AccessKey());
            conf.set(AbstractS3FileSystemFactory.SECRET_KEY, S3TestCredentials.getS3SecretKey());

            FileSystem.initialize(conf);
            path.getFileSystem().exists(path);
        }

        // shortened Hadoop-style credential keys
        {
            Configuration conf = new Configuration();
            conf.setString(S3_USE_INSTANCE_CREDENTIALS, "false");
            conf.setString("s3.access.key", S3TestCredentials.getS3AccessKey());
            conf.setString("s3.secret.key", S3TestCredentials.getS3SecretKey());

            FileSystem.initialize(conf);
            path.getFileSystem().exists(path);
        }

        // shortened Hadoop-style credential keys with presto prefix
        {
            Configuration conf = new Configuration();
            conf.setString(S3_USE_INSTANCE_CREDENTIALS, "false");
            conf.setString("presto.s3.access.key", S3TestCredentials.getS3AccessKey());
            conf.setString("presto.s3.secret.key", S3TestCredentials.getS3SecretKey());

            FileSystem.initialize(conf);
            path.getFileSystem().exists(path);
        }

        // re-set configuration
        FileSystem.initialize(new Configuration());
    }

    @Override
    protected void checkEmptyDirectory(Path path) throws IOException, InterruptedException {
        // seems the presto file system does not assume existence of empty directories in S3
        // do nothing as before
    }
}
