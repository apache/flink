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

package org.apache.flink.table.sql.codegen;

import org.apache.flink.test.resources.ResourceTestUtils;
import org.apache.flink.test.util.SQLJobSubmission;
import org.apache.flink.tests.util.flink.ClusterController;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.OperatingSystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

/** Base class for sql ITCase which depends on HDFS env. */
public abstract class HdfsITCaseBase extends SqlITCaseBase {

    private static final Path HADOOP_CLASSPATH =
            ResourceTestUtils.getResource(".*hadoop.classpath");

    protected Configuration hdConf;
    protected MiniDFSCluster hdfsCluster;

    public HdfsITCaseBase(String executionMode) {
        super(executionMode);
    }

    @BeforeClass
    public static void verifyOS() {
        Assume.assumeTrue(
                "HDFS cluster cannot be started on Windows without extensions.",
                !OperatingSystem.isWindows());
    }

    @Before
    public void before() throws Exception {
        super.before();
        createHDFS();
    }

    @After
    public void after() {
        destroyHDFS();
    }

    protected void createHDFS() {
        try {
            hdConf = new Configuration();
            File baseDir =
                    new File(String.format("./target/hdfs/%s", getClass().getSimpleName()))
                            .getAbsoluteFile();
            FileUtil.fullyDelete(baseDir);
            hdConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
            MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(hdConf);
            hdfsCluster = builder.build();
        } catch (Throwable e) {
            Assert.fail("Failed to create HDFS env" + e.getMessage());
        }
    }

    protected void destroyHDFS() {
        hdfsCluster.shutdown();
    }

    @Override
    protected void executeSqlStatements(ClusterController clusterController, List<String> sqlLines)
            throws Exception {
        clusterController.submitSQLJob(
                new SQLJobSubmission.SQLJobSubmissionBuilder(sqlLines)
                        .setEnvProcessor(
                                map -> map.put("HADOOP_CLASSPATH", getHadoopClassPathContent()))
                        .build(),
                Duration.ofMinutes(2L));
    }

    private String getHadoopClassPathContent() {
        // Prepare all hadoop jars to mock HADOOP_CLASSPATH, use hadoop.classpath which contains all
        // hadoop jars
        File hadoopClasspathFile = new File(HADOOP_CLASSPATH.toAbsolutePath().toString());

        if (!hadoopClasspathFile.exists()) {
            try {
                throw new FileNotFoundException(
                        String.format(
                                "File that contains hadoop classpath %s does not exist.",
                                HADOOP_CLASSPATH));
            } catch (FileNotFoundException e) {
                Assert.fail("Test failed " + e.getMessage());
            }
        }

        String classPathContent = null;
        try {
            classPathContent = FileUtils.readFileUtf8(hadoopClasspathFile);
        } catch (IOException e) {
            Assert.fail("Test failed " + e.getMessage());
        }
        return classPathContent;
    }
}
