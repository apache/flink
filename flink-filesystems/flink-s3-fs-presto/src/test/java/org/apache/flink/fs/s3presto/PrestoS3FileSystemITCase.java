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
import org.apache.flink.core.testutils.AllCallbackWrapper;
import org.apache.flink.core.testutils.TestContainerExtension;
import org.apache.flink.fs.s3.common.MinioTestContainer;
import org.apache.flink.runtime.fs.hdfs.AbstractHadoopFileSystemITTest;

import com.amazonaws.SdkClientException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.util.UUID;

import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_USE_INSTANCE_CREDENTIALS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Unit tests for the S3 file system support via Presto's {@link
 * com.facebook.presto.hive.s3.PrestoS3FileSystem}.
 *
 * <p><strong>BEWARE</strong>: tests must take special care of S3's <a
 * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html#ConsistencyModel">consistency
 * guarantees</a> and what the {@link com.facebook.presto.hive.s3.PrestoS3FileSystem} offers.
 */
// @RunWith(Parameterized.class)
public class PrestoS3FileSystemITCase extends AbstractHadoopFileSystemITTest {

    //    @Parameterized.Parameter public String scheme;

    //    @Parameterized.Parameters(name = "Scheme = {0}")
    //    public static List<String> parameters() {
    //        return Arrays.asList("s3", "s3p");
    //    }

    private static final String TEST_DATA_DIR = "tests-" + UUID.randomUUID();

    @RegisterExtension
    private static final AllCallbackWrapper<TestContainerExtension<MinioTestContainer>>
            MINIO_EXTENSION =
                    new AllCallbackWrapper<>(new TestContainerExtension<>(MinioTestContainer::new));

    @BeforeAll
    static void setup() throws IOException {
        final Configuration conf = new Configuration();
        MINIO_EXTENSION.getCustomExtension().getTestContainer().setS3ConfigOptions(conf);
        MINIO_EXTENSION.getCustomExtension().getTestContainer().initializeFileSystem(conf);

        consistencyToleranceNS = 30_000_000_000L; // 30 seconds
    }

    @Override
    protected FileSystem getFileSystem() throws IOException {
        return getBasePath().getFileSystem();
    }

    @Override
    protected Path getBasePath() {
        return new Path(
                MINIO_EXTENSION.getCustomExtension().getTestContainer().getS3UriForDefaultBucket()
                        + "/temp/"
                        + TEST_DATA_DIR);
    }

    @Test
    public void testConfigKeysForwarding() throws Exception {
        final Path path = getBasePath();

        // access without credentials should fail
        {
            Configuration conf = new Configuration();
            // fail fast and do not fall back to trying EC2 credentials
            conf.setString(S3_USE_INSTANCE_CREDENTIALS, "false");
            FileSystem.initialize(conf);

            try {
                path.getFileSystem().exists(path);
                fail("should fail with an exception");
            } catch (SdkClientException ignored) {
            }
        }

        // standard Presto-style credential keys
        {
            Configuration conf = new Configuration();
            conf.setString(S3_USE_INSTANCE_CREDENTIALS, "false");
            conf.setString("s3.endpoint", getMinioContainer().getHttpEndpoint());
            conf.setString("s3.path.style.access", "true");
            conf.setString("presto.s3.access-key", getMinioContainer().getAccessKey());
            conf.setString("presto.s3.secret-key", getMinioContainer().getSecretKey());

            getMinioContainer().initializeFileSystem(conf);
            assertThat(getFileSystem().exists(path)).isFalse();
        }

        // shortened Presto-style credential keys
        {
            Configuration conf = new Configuration();
            conf.setString(S3_USE_INSTANCE_CREDENTIALS, "false");
            conf.setString("s3.endpoint", getMinioContainer().getHttpEndpoint());
            conf.setString("s3.path.style.access", "true");
            conf.setString("s3.access-key", getMinioContainer().getAccessKey());
            conf.setString("s3.secret-key", getMinioContainer().getSecretKey());

            getMinioContainer().initializeFileSystem(conf);
            assertThat(getFileSystem().exists(path)).isFalse();
        }

        // shortened Hadoop-style credential keys
        {
            Configuration conf = new Configuration();
            conf.setString(S3_USE_INSTANCE_CREDENTIALS, "false");
            conf.setString("s3.endpoint", getMinioContainer().getHttpEndpoint());
            conf.setString("s3.path.style.access", "true");
            conf.setString("s3.access.key", getMinioContainer().getAccessKey());
            conf.setString("s3.secret.key", getMinioContainer().getSecretKey());

            getMinioContainer().initializeFileSystem(conf);
            assertThat(getFileSystem().exists(path)).isFalse();
        }

        // shortened Hadoop-style credential keys with presto prefix
        {
            Configuration conf = new Configuration();
            conf.setString(S3_USE_INSTANCE_CREDENTIALS, "false");
            conf.setString("s3.endpoint", getMinioContainer().getHttpEndpoint());
            conf.setString("s3.path.style.access", "true");
            conf.setString("presto.s3.access.key", getMinioContainer().getAccessKey());
            conf.setString("presto.s3.secret.key", getMinioContainer().getSecretKey());

            getMinioContainer().initializeFileSystem(conf);
            assertThat(getFileSystem().exists(path)).isFalse();
        }

        // re-set configuration
        FileSystem.initialize(new Configuration());
    }

    @Override
    protected void checkEmptyDirectory(Path path) throws IOException, InterruptedException {
        // seems the presto file system does not assume existence of empty directories in S3
        // do nothing as before
    }

    private static MinioTestContainer getMinioContainer() {
        return MINIO_EXTENSION.getCustomExtension().getTestContainer();
    }
}
