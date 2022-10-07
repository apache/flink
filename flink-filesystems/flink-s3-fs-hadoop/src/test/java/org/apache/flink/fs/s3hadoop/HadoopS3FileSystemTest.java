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
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.util.HadoopConfigLoader;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.runtime.util.ConfigurationFileUtil.printConfigs;
import static org.junit.Assert.assertEquals;

/**
 * Unit tests for the S3 file system support via Hadoop's {@link
 * org.apache.hadoop.fs.s3a.S3AFileSystem}.
 */
public class HadoopS3FileSystemTest {
    @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testShadingOfAwsCredProviderConfig() {
        final Configuration conf = new Configuration();
        conf.setString(
                "fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.ContainerCredentialsProvider");

        HadoopConfigLoader configLoader = S3FileSystemFactory.createHadoopConfigLoader();
        configLoader.setFlinkConfig(conf);

        org.apache.hadoop.conf.Configuration hadoopConfig = configLoader.getOrLoadHadoopConfig();
        assertEquals(
                "com.amazonaws.auth.ContainerCredentialsProvider",
                hadoopConfig.get("fs.s3a.aws.credentials.provider"));
    }

    // ------------------------------------------------------------------------
    //  These tests check that the S3FileSystemFactory properly forwards
    // various patterns of keys for credentials.
    // ------------------------------------------------------------------------

    /** Test forwarding of standard Hadoop-style credential keys. */
    @Test
    public void testConfigKeysForwardingHadoopStyle() {
        Configuration conf = new Configuration();
        conf.setString("fs.s3a.access.key", "test_access_key");
        conf.setString("fs.s3a.secret.key", "test_secret_key");

        checkHadoopAccessKeys(conf, "test_access_key", "test_secret_key");
    }

    /** Test forwarding of shortened Hadoop-style credential keys. */
    @Test
    public void testConfigKeysForwardingShortHadoopStyle() {
        Configuration conf = new Configuration();
        conf.setString("s3.access.key", "my_key_a");
        conf.setString("s3.secret.key", "my_key_b");

        checkHadoopAccessKeys(conf, "my_key_a", "my_key_b");
    }

    /** Test forwarding of shortened Presto-style credential keys. */
    @Test
    public void testConfigKeysForwardingPrestoStyle() {
        Configuration conf = new Configuration();
        conf.setString("s3.access-key", "clé d'accès");
        conf.setString("s3.secret-key", "clef secrète");
        checkHadoopAccessKeys(conf, "clé d'accès", "clef secrète");
    }

    /** Test reading the values from the base hadoop configuration. */
    @Test
    public void testConfigKeysFromHadoopConfig() throws IOException {
        final File confDir = tempFolder.newFolder();
        final File file1 = new File(confDir, "core-site.xml");

        printConfigs(
                file1,
                ImmutableMap.of(
                        "fs.s3a.overwrite", "from.hadoop.conf",
                        "fs.s3a.orig", "from.hadoop.conf"));

        final Configuration conf = new Configuration();
        conf.setString("fs.s3a.from.conf", "from.conf");
        conf.setString("fs.s3a.overwrite", "from.conf");

        final Map<String, String> originalEnv = System.getenv();
        final Map<String, String> newEnv = new HashMap<>(originalEnv);
        newEnv.put("HADOOP_CONF_DIR", confDir.getAbsolutePath());
        try {
            CommonTestUtils.setEnv(newEnv);
            HadoopConfigLoader configLoader = S3FileSystemFactory.createHadoopConfigLoader();
            configLoader.setFlinkConfig(conf);
            org.apache.hadoop.conf.Configuration hadoopConf = configLoader.getOrLoadHadoopConfig();
            assertEquals("from.conf", hadoopConf.get("fs.s3a.from.conf", null));
            assertEquals("from.conf", hadoopConf.get("fs.s3a.overwrite", null));
            assertEquals("from.hadoop.conf", hadoopConf.get("fs.s3a.orig", null));
        } finally {
            CommonTestUtils.setEnv(originalEnv);
        }
    }

    private static void checkHadoopAccessKeys(
            Configuration flinkConf, String accessKey, String secretKey) {
        HadoopConfigLoader configLoader = S3FileSystemFactory.createHadoopConfigLoader();
        configLoader.setFlinkConfig(flinkConf);

        org.apache.hadoop.conf.Configuration hadoopConf = configLoader.getOrLoadHadoopConfig();

        assertEquals(accessKey, hadoopConf.get("fs.s3a.access.key", null));
        assertEquals(secretKey, hadoopConf.get("fs.s3a.secret.key", null));
    }
}
