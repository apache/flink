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

package org.apache.flink.yarn;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.testutils.junit.RetryOnFailure;
import org.apache.flink.testutils.junit.RetryRule;
import org.apache.flink.testutils.s3.S3TestCredentials;
import org.apache.flink.util.TestLogger;

import org.apache.hadoop.util.VersionUtil;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeNoException;

/**
 * Tests for verifying file staging during submission to YARN works with the S3A file system.
 *
 * <p>Note that the setup is similar to
 * <tt>org.apache.flink.fs.s3hadoop.HadoopS3FileSystemITCase</tt>.
 */
public class YarnFileStageTestS3ITCase extends TestLogger {

    private static final String TEST_DATA_DIR = "tests-" + UUID.randomUUID();

    @ClassRule public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

    @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

    @Rule public final RetryRule retryRule = new RetryRule();

    /** Number of tests executed. */
    private static int numRecursiveUploadTests = 0;

    /** Will be updated by {@link #checkCredentialsAndSetup()} if the test is not skipped. */
    private static boolean skipTest = true;

    @BeforeClass
    public static void checkCredentialsAndSetup() throws IOException {
        // check whether credentials exist
        S3TestCredentials.assumeCredentialsAvailable();

        skipTest = false;

        setupCustomHadoopConfig();
    }

    @AfterClass
    public static void resetFileSystemConfiguration() throws IOException {
        FileSystem.initialize(new Configuration());
    }

    @AfterClass
    public static void checkAtLeastOneTestRun() {
        if (!skipTest) {
            assertThat(
                    "No S3 filesystem upload test executed. Please activate the "
                            + "'include_hadoop_aws' build profile or set '-Dinclude_hadoop_aws' during build "
                            + "(Hadoop >= 2.6 moved S3 filesystems out of hadoop-common).",
                    numRecursiveUploadTests,
                    greaterThan(0));
        }
    }

    /**
     * Create a Hadoop config file containing S3 access credentials.
     *
     * <p>Note that we cannot use them as part of the URL since this may fail if the credentials
     * contain a "/" (see <a
     * href="https://issues.apache.org/jira/browse/HADOOP-3733">HADOOP-3733</a>).
     */
    private static void setupCustomHadoopConfig() throws IOException {
        File hadoopConfig = TEMP_FOLDER.newFile();
        Map<String /* key */, String /* value */> parameters = new HashMap<>();

        // set all different S3 fs implementation variants' configuration keys
        parameters.put("fs.s3a.access.key", S3TestCredentials.getS3AccessKey());
        parameters.put("fs.s3a.secret.key", S3TestCredentials.getS3SecretKey());

        parameters.put("fs.s3.awsAccessKeyId", S3TestCredentials.getS3AccessKey());
        parameters.put("fs.s3.awsSecretAccessKey", S3TestCredentials.getS3SecretKey());

        parameters.put("fs.s3n.awsAccessKeyId", S3TestCredentials.getS3AccessKey());
        parameters.put("fs.s3n.awsSecretAccessKey", S3TestCredentials.getS3SecretKey());

        try (PrintStream out = new PrintStream(new FileOutputStream(hadoopConfig))) {
            out.println("<?xml version=\"1.0\"?>");
            out.println("<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>");
            out.println("<configuration>");
            for (Map.Entry<String, String> entry : parameters.entrySet()) {
                out.println("\t<property>");
                out.println("\t\t<name>" + entry.getKey() + "</name>");
                out.println("\t\t<value>" + entry.getValue() + "</value>");
                out.println("\t</property>");
            }
            out.println("</configuration>");
        }

        final Configuration conf = new Configuration();
        conf.setString(ConfigConstants.HDFS_SITE_CONFIG, hadoopConfig.getAbsolutePath());
        conf.set(CoreOptions.ALLOWED_FALLBACK_FILESYSTEMS, "s3;s3a;s3n");

        FileSystem.initialize(conf);
    }

    /**
     * Verifies that nested directories are properly copied with to the given S3 path (using the
     * appropriate file system) during resource uploads for YARN.
     *
     * @param scheme file system scheme
     * @param pathSuffix test path suffix which will be the test's target path
     */
    private void testRecursiveUploadForYarn(String scheme, String pathSuffix) throws Exception {
        ++numRecursiveUploadTests;

        final Path basePath =
                new Path(S3TestCredentials.getTestBucketUriWithScheme(scheme) + TEST_DATA_DIR);
        final HadoopFileSystem fs = (HadoopFileSystem) basePath.getFileSystem();

        assumeFalse(fs.exists(basePath));

        try {
            final Path directory = new Path(basePath, pathSuffix);

            YarnFileStageTest.testRegisterMultipleLocalResources(
                    fs.getHadoopFileSystem(),
                    new org.apache.hadoop.fs.Path(directory.toUri()),
                    Path.CUR_DIR,
                    tempFolder,
                    false,
                    false);
        } finally {
            // clean up
            fs.delete(basePath, true);
        }
    }

    @Test
    @RetryOnFailure(times = 3)
    public void testRecursiveUploadForYarnS3n() throws Exception {
        // skip test on Hadoop 3: https://issues.apache.org/jira/browse/HADOOP-14738
        Assume.assumeTrue(
                "This test is skipped for Hadoop versions above 3",
                VersionUtil.compareVersions(System.getProperty("hadoop.version"), "3.0.0") < 0);

        try {
            Class.forName("org.apache.hadoop.fs.s3native.NativeS3FileSystem");
        } catch (ClassNotFoundException e) {
            // not in the classpath, cannot run this test
            String msg = "Skipping test because NativeS3FileSystem is not in the class path";
            log.info(msg);
            assumeNoException(msg, e);
        }
        testRecursiveUploadForYarn("s3n", "testYarn-s3n");
    }

    @Test
    @RetryOnFailure(times = 3)
    public void testRecursiveUploadForYarnS3a() throws Exception {
        try {
            Class.forName("org.apache.hadoop.fs.s3a.S3AFileSystem");
        } catch (ClassNotFoundException e) {
            // not in the classpath, cannot run this test
            String msg = "Skipping test because S3AFileSystem is not in the class path";
            log.info(msg);
            assumeNoException(msg, e);
        }
        testRecursiveUploadForYarn("s3a", "testYarn-s3a");
    }
}
