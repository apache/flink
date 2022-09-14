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
import org.apache.flink.fs.s3.common.FlinkS3FileSystem;
import org.apache.flink.runtime.util.HadoopConfigLoader;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.facebook.presto.hive.s3.PrestoS3FileSystem;
import org.junit.Test;

import java.lang.reflect.Field;
import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for the S3 file system support via Presto's PrestoS3FileSystem. These tests do not
 * actually read from or write to S3.
 */
public class PrestoS3FileSystemTest {

    @Test
    public void testConfigPropagation() throws Exception {
        final Configuration conf = new Configuration();
        conf.setString("s3.access-key", "test_access_key_id");
        conf.setString("s3.secret-key", "test_secret_access_key");

        FileSystem.initialize(conf);

        FileSystem fs = FileSystem.get(new URI("s3://test"));
        validateBasicCredentials(fs);
    }

    @Test
    public void testConfigPropagationWithPrestoPrefix() throws Exception {
        final Configuration conf = new Configuration();
        conf.setString("presto.s3.access-key", "test_access_key_id");
        conf.setString("presto.s3.secret-key", "test_secret_access_key");

        FileSystem.initialize(conf);

        FileSystem fs = FileSystem.get(new URI("s3://test"));
        validateBasicCredentials(fs);
    }

    @Test
    public void testConfigPropagationAlternateStyle() throws Exception {
        final Configuration conf = new Configuration();
        conf.setString("s3.access.key", "test_access_key_id");
        conf.setString("s3.secret.key", "test_secret_access_key");

        FileSystem.initialize(conf);

        FileSystem fs = FileSystem.get(new URI("s3://test"));
        validateBasicCredentials(fs);
    }

    @Test
    public void testShadingOfAwsCredProviderConfig() {
        final Configuration conf = new Configuration();
        conf.setString(
                "presto.s3.credentials-provider",
                "com.amazonaws.auth.ContainerCredentialsProvider");

        HadoopConfigLoader configLoader = S3FileSystemFactory.createHadoopConfigLoader();
        configLoader.setFlinkConfig(conf);

        org.apache.hadoop.conf.Configuration hadoopConfig = configLoader.getOrLoadHadoopConfig();
        assertEquals(
                "com.amazonaws.auth.ContainerCredentialsProvider",
                hadoopConfig.get("presto.s3.credentials-provider"));
    }

    // ------------------------------------------------------------------------
    //  utilities
    // ------------------------------------------------------------------------

    private static void validateBasicCredentials(FileSystem fs) throws Exception {
        assertTrue(fs instanceof FlinkS3FileSystem);

        org.apache.hadoop.fs.FileSystem hadoopFs = ((FlinkS3FileSystem) fs).getHadoopFileSystem();
        assertTrue(hadoopFs instanceof PrestoS3FileSystem);

        try (PrestoS3FileSystem prestoFs = (PrestoS3FileSystem) hadoopFs) {
            AWSCredentialsProvider provider = getAwsCredentialsProvider(prestoFs);
            assertTrue(provider instanceof AWSStaticCredentialsProvider);
        }
    }

    private static AWSCredentialsProvider getAwsCredentialsProvider(PrestoS3FileSystem fs)
            throws Exception {
        Field amazonS3field = PrestoS3FileSystem.class.getDeclaredField("s3");
        amazonS3field.setAccessible(true);
        AmazonS3Client amazonS3 = (AmazonS3Client) amazonS3field.get(fs);

        Field providerField = AmazonS3Client.class.getDeclaredField("awsCredentialsProvider");
        providerField.setAccessible(true);
        return (AWSCredentialsProvider) providerField.get(amazonS3);
    }
}
