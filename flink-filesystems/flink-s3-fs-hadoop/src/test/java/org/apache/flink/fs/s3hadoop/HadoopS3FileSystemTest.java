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
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.runtime.util.HadoopConfigLoader;

import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.ServiceLoader;

import static org.apache.hadoop.fs.s3a.Constants.ASSUMED_ROLE_ARN;
import static org.apache.hadoop.fs.s3a.Constants.ASSUMED_ROLE_CREDENTIALS_PROVIDER;
import static org.apache.hadoop.fs.s3a.Constants.AWS_CREDENTIALS_PROVIDER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Unit tests for the S3 file system support via Hadoop's {@link
 * org.apache.hadoop.fs.s3a.S3AFileSystem}.
 */
public class HadoopS3FileSystemTest {

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

    @Test
    public void testInitUriWithParams() throws URISyntaxException {
        ServiceLoader<FileSystemFactory> serviceLoader =
                ServiceLoader.load(FileSystemFactory.class);
        S3FileSystemFactory s3Factory = null;
        for (FileSystemFactory fs : serviceLoader) {
            if ("s3".equals(fs.getScheme())) {
                s3Factory = (S3FileSystemFactory) fs;
                break;
            }
        }
        assertNotNull("S3FileSystemFactory must be set", s3Factory);

        String assumedRoleProvider = "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider";
        String simpleProvider = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider";
        String roleArn = "arn:aws:iam::000000000000:role/some-role";
        URI uri =
                new URI(
                        "s3://bucket"
                                + "?fs.s3a.aws.credentials.provider="
                                + assumedRoleProvider
                                + "&fs.s3a.assumed.role.credentials.provider="
                                + simpleProvider
                                + "&fs.s3a.assumed.role.arn="
                                + roleArn);

        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        URI resultUri = s3Factory.getInitURI(uri, conf);

        assertEquals("s3://bucket", resultUri.toString());
        assertEquals(assumedRoleProvider, conf.get(AWS_CREDENTIALS_PROVIDER));
        assertEquals(simpleProvider, conf.get(ASSUMED_ROLE_CREDENTIALS_PROVIDER));
        assertEquals(roleArn, conf.get(ASSUMED_ROLE_ARN));
    }

    @Test
    public void testParseQueryParams() throws URISyntaxException {
        URI uri1 = new URI("s3://bucket/path");
        Map<String, String> paramMap = S3FileSystemFactory.parseQueryParams(uri1);
        assertEquals(0, paramMap.size());

        URI uri2 = new URI("s3://bucket/path?foo=bar&baz=bot");
        paramMap = S3FileSystemFactory.parseQueryParams(uri2);
        assertEquals("bar", paramMap.get("foo"));
        assertEquals("bot", paramMap.get("baz"));
        assertEquals(2, paramMap.size());
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
