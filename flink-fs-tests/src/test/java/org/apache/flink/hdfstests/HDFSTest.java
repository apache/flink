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

package org.apache.flink.hdfstests;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironmentFactory;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.examples.java.wordcount.WordCount;
import org.apache.flink.runtime.blob.BlobStoreService;
import org.apache.flink.runtime.blob.BlobUtils;
import org.apache.flink.runtime.blob.TestingBlobHelpers;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.util.OperatingSystem;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;

import static org.junit.Assert.assertTrue;

/**
 * This test should logically be located in the 'flink-runtime' tests. However, this project has
 * already all dependencies required (flink-java-examples). Also, the ParallelismOneExecEnv is here.
 */
public class HDFSTest {

    protected String hdfsURI;
    private MiniDFSCluster hdfsCluster;
    private org.apache.hadoop.fs.Path hdPath;
    protected org.apache.hadoop.fs.FileSystem hdfs;

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @BeforeClass
    public static void verifyOS() {
        Assume.assumeTrue(
                "HDFS cluster cannot be started on Windows without extensions.",
                !OperatingSystem.isWindows());
    }

    @Before
    public void createHDFS() {
        try {
            Configuration hdConf = new Configuration();

            File baseDir = new File("./target/hdfs/hdfsTest").getAbsoluteFile();
            FileUtil.fullyDelete(baseDir);
            hdConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
            MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(hdConf);
            hdfsCluster = builder.build();

            hdfsURI =
                    "hdfs://"
                            + hdfsCluster.getURI().getHost()
                            + ":"
                            + hdfsCluster.getNameNodePort()
                            + "/";

            hdPath = new org.apache.hadoop.fs.Path("/test");
            hdfs = hdPath.getFileSystem(hdConf);
            FSDataOutputStream stream = hdfs.create(hdPath);
            for (int i = 0; i < 10; i++) {
                stream.write("Hello HDFS\n".getBytes(ConfigConstants.DEFAULT_CHARSET));
            }
            stream.close();

        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail("Test failed " + e.getMessage());
        }
    }

    @After
    public void destroyHDFS() {
        try {
            hdfs.delete(hdPath, false);
            hdfsCluster.shutdown();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testHDFS() {

        Path file = new Path(hdfsURI + hdPath);
        org.apache.hadoop.fs.Path result = new org.apache.hadoop.fs.Path(hdfsURI + "/result");
        try {
            FileSystem fs = file.getFileSystem();
            assertTrue("Must be HadoopFileSystem", fs instanceof HadoopFileSystem);

            DopOneTestEnvironment.setAsContext();
            try {
                WordCount.main(
                        new String[] {
                            "--input", file.toString(),
                            "--output", result.toString()
                        });
            } catch (Throwable t) {
                t.printStackTrace();
                Assert.fail("Test failed with " + t.getMessage());
            } finally {
                DopOneTestEnvironment.unsetAsContext();
            }

            assertTrue("No result file present", hdfs.exists(result));

            // validate output:
            org.apache.hadoop.fs.FSDataInputStream inStream = hdfs.open(result);
            StringWriter writer = new StringWriter();
            IOUtils.copy(inStream, writer);
            String resultString = writer.toString();

            Assert.assertEquals("hdfs 10\n" + "hello 10\n", resultString);
            inStream.close();

        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Error in test: " + e.getMessage());
        }
    }

    @Test
    public void testChangingFileNames() {
        org.apache.hadoop.fs.Path hdfsPath = new org.apache.hadoop.fs.Path(hdfsURI + "/hdfsTest");
        Path path = new Path(hdfsPath.toString());

        String type = "one";
        TextOutputFormat<String> outputFormat = new TextOutputFormat<>(path);

        outputFormat.setWriteMode(FileSystem.WriteMode.NO_OVERWRITE);
        outputFormat.setOutputDirectoryMode(FileOutputFormat.OutputDirectoryMode.ALWAYS);

        try {
            outputFormat.open(0, 2);
            outputFormat.writeRecord(type);
            outputFormat.close();

            outputFormat.open(1, 2);
            outputFormat.writeRecord(type);
            outputFormat.close();

            assertTrue("No result file present", hdfs.exists(hdfsPath));
            FileStatus[] files = hdfs.listStatus(hdfsPath);
            Assert.assertEquals(2, files.length);
            for (FileStatus file : files) {
                assertTrue(
                        "1".equals(file.getPath().getName())
                                || "2".equals(file.getPath().getName()));
            }

        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Tests that with {@link HighAvailabilityMode#ZOOKEEPER} distributed JARs are recoverable from
     * any participating BlobServer when talking to the {@link
     * org.apache.flink.runtime.blob.BlobServer} directly.
     */
    @Test
    public void testBlobServerRecovery() throws Exception {
        org.apache.flink.configuration.Configuration config =
                new org.apache.flink.configuration.Configuration();
        config.setString(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
        config.setString(
                BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());
        config.setString(HighAvailabilityOptions.HA_STORAGE_PATH, hdfsURI);

        BlobStoreService blobStoreService = BlobUtils.createBlobStoreFromConfig(config);

        try {
            TestingBlobHelpers.testBlobServerRecovery(
                    config, blobStoreService, temporaryFolder.newFolder());
        } finally {
            blobStoreService.closeAndCleanupAllData();
        }
    }

    /**
     * Tests that with {@link HighAvailabilityMode#ZOOKEEPER} distributed corrupted JARs are
     * recognised during the download via a {@link org.apache.flink.runtime.blob.BlobServer}.
     */
    @Test
    public void testBlobServerCorruptedFile() throws Exception {
        org.apache.flink.configuration.Configuration config =
                new org.apache.flink.configuration.Configuration();
        config.setString(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
        config.setString(
                BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());
        config.setString(HighAvailabilityOptions.HA_STORAGE_PATH, hdfsURI);

        BlobStoreService blobStoreService = BlobUtils.createBlobStoreFromConfig(config);

        try {
            TestingBlobHelpers.testGetFailsFromCorruptFile(
                    config, blobStoreService, temporaryFolder.newFolder());
        } finally {
            blobStoreService.closeAndCleanupAllData();
        }
    }

    /**
     * Tests that with {@link HighAvailabilityMode#ZOOKEEPER} distributed JARs are recoverable from
     * any participating BlobServer when uploaded via a BLOB cache.
     */
    @Test
    public void testBlobCacheRecovery() throws Exception {
        org.apache.flink.configuration.Configuration config =
                new org.apache.flink.configuration.Configuration();
        config.setString(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
        config.setString(
                BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());
        config.setString(HighAvailabilityOptions.HA_STORAGE_PATH, hdfsURI);

        BlobStoreService blobStoreService = BlobUtils.createBlobStoreFromConfig(config);

        try {
            TestingBlobHelpers.testBlobCacheRecovery(
                    config, blobStoreService, temporaryFolder.newFolder());
        } finally {
            blobStoreService.closeAndCleanupAllData();
        }
    }

    /**
     * Tests that with {@link HighAvailabilityMode#ZOOKEEPER} distributed corrupted JARs are
     * recognised during the download via a BLOB cache.
     */
    @Test
    public void testBlobCacheCorruptedFile() throws Exception {
        org.apache.flink.configuration.Configuration config =
                new org.apache.flink.configuration.Configuration();
        config.setString(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
        config.setString(
                BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());
        config.setString(HighAvailabilityOptions.HA_STORAGE_PATH, hdfsURI);

        BlobStoreService blobStoreService = BlobUtils.createBlobStoreFromConfig(config);

        try {
            TestingBlobHelpers.testGetFailsFromCorruptFile(
                    new JobID(), config, blobStoreService, temporaryFolder.newFolder());
        } finally {
            blobStoreService.closeAndCleanupAllData();
        }
    }

    abstract static class DopOneTestEnvironment extends ExecutionEnvironment {

        public static void setAsContext() {
            final LocalEnvironment le = new LocalEnvironment();
            le.setParallelism(1);

            initializeContextEnvironment(
                    new ExecutionEnvironmentFactory() {

                        @Override
                        public ExecutionEnvironment createExecutionEnvironment() {
                            return le;
                        }
                    });
        }

        public static void unsetAsContext() {
            resetContextEnvironment();
        }
    }
}
