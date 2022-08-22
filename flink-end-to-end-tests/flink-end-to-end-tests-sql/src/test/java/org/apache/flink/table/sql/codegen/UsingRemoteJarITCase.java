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

import org.apache.flink.test.util.SQLJobSubmission;
import org.apache.flink.tests.util.TestUtils;
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
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/** End to End tests for using remote jar. */
public class UsingRemoteJarITCase extends SqlITCaseBase {
    private static final Path HADOOP_CLASSPATH = TestUtils.getResource(".*hadoop.classpath");

    private MiniDFSCluster hdfsCluster;
    private org.apache.hadoop.fs.Path hdPath;
    private org.apache.hadoop.fs.FileSystem hdfs;

    public UsingRemoteJarITCase(String executionMode) {
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

    private void createHDFS() {
        try {
            Configuration hdConf = new Configuration();

            File baseDir = new File("./target/hdfs/hdfsTest").getAbsoluteFile();
            FileUtil.fullyDelete(baseDir);
            hdConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
            MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(hdConf);
            hdfsCluster = builder.build();

            hdPath = new org.apache.hadoop.fs.Path("/test.jar");
            hdfs = hdPath.getFileSystem(hdConf);

            hdfs.copyFromLocalFile(
                    new org.apache.hadoop.fs.Path(SQL_TOOL_BOX_JAR.toString()), hdPath);
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
    public void testUdfInRemoteJar() throws Exception {
        runAndCheckSQL(
                "remote_jar_e2e.sql",
                generateReplaceVars(),
                2,
                Arrays.asList(
                        "{\"before\":null,\"after\":{\"user_name\":\"Alice\",\"order_cnt\":1},\"op\":\"c\"}",
                        "{\"before\":null,\"after\":{\"user_name\":\"Bob\",\"order_cnt\":2},\"op\":\"c\"}"));
    }

    @Test
    public void testCreateTemporarySystemFunctionUsingRemoteJar() throws Exception {
        Map<String, String> replaceVars = generateReplaceVars();
        replaceVars.put("$TEMPORARY", "TEMPORARY SYSTEM");
        runAndCheckSQL(
                "create_function_using_remote_jar_e2e.sql",
                replaceVars,
                2,
                Arrays.asList(
                        "{\"before\":null,\"after\":{\"user_name\":\"Alice\",\"order_cnt\":1},\"op\":\"c\"}",
                        "{\"before\":null,\"after\":{\"user_name\":\"Bob\",\"order_cnt\":2},\"op\":\"c\"}"));
    }

    @Test
    public void testCreateCatalogFunctionUsingRemoteJar() throws Exception {
        Map<String, String> replaceVars = generateReplaceVars();
        replaceVars.put("$TEMPORARY", "");
        runAndCheckSQL(
                "create_function_using_remote_jar_e2e.sql",
                replaceVars,
                2,
                Arrays.asList(
                        "{\"before\":null,\"after\":{\"user_name\":\"Alice\",\"order_cnt\":1},\"op\":\"c\"}",
                        "{\"before\":null,\"after\":{\"user_name\":\"Bob\",\"order_cnt\":2},\"op\":\"c\"}"));
    }

    @Test
    public void testCreateTemporaryCatalogFunctionUsingRemoteJar() throws Exception {
        Map<String, String> replaceVars = generateReplaceVars();
        replaceVars.put("$TEMPORARY", "TEMPORARY");
        runAndCheckSQL(
                "create_function_using_remote_jar_e2e.sql",
                replaceVars,
                2,
                Arrays.asList(
                        "{\"before\":null,\"after\":{\"user_name\":\"Alice\",\"order_cnt\":1},\"op\":\"c\"}",
                        "{\"before\":null,\"after\":{\"user_name\":\"Bob\",\"order_cnt\":2},\"op\":\"c\"}"));
    }

    @Override
    protected Map<String, String> generateReplaceVars() {
        String remoteJarPath =
                String.format(
                        "hdfs://%s:%s/%s",
                        hdfsCluster.getURI().getHost(), hdfsCluster.getNameNodePort(), hdPath);

        Map<String, String> map = super.generateReplaceVars();
        map.put("$JAR_PATH", remoteJarPath);
        return map;
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
                e.printStackTrace();
                Assert.fail("Test failed " + e.getMessage());
            }
        }

        String classPathContent = null;
        try {
            classPathContent = FileUtils.readFileUtf8(hadoopClasspathFile);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Test failed " + e.getMessage());
        }
        return classPathContent;
    }
}
