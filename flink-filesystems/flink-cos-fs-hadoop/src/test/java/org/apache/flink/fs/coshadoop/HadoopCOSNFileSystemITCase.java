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

package org.apache.flink.fs.coshadoop;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.fs.hdfs.AbstractHadoopFileSystemITTest;
import org.apache.flink.testutils.cos.COSTestCredentials;

import org.junit.BeforeClass;

import java.io.IOException;
import java.util.UUID;

/**
 * Unit tests for the COSN file system support via CosNFileSystem. These tests do actually read from
 * or write to COS.
 */
public class HadoopCOSNFileSystemITCase extends AbstractHadoopFileSystemITTest {

    private static final String TEST_DATA_DIR = "tests-" + UUID.randomUUID();

    @BeforeClass
    public static void setup() throws IOException {
        COSTestCredentials.assumeCredentialsAvailable();

        final Configuration conf = new Configuration();
        conf.setString("fs.cosn.userinfo.secretId", COSTestCredentials.getCosTestSecretId());
        conf.setString("fs.cosn.userinfo.secretKey", COSTestCredentials.getCosTestSecretKey());
        conf.setString("fs.cosn.impl", COSTestCredentials.getCosTestCosnImpl());
        conf.setString(
                "fs.AbstractFileSystem.cosn.impl", COSTestCredentials.getCosTestAfsCosnImpl());
        conf.setString("fs.cosn.bucket.region", COSTestCredentials.getCosTestRegion());

        FileSystem.initialize(conf);
        basePath = new Path(COSTestCredentials.getTestBucketUri() + TEST_DATA_DIR);
        fs = basePath.getFileSystem();
        consistencyToleranceNS = 0;
    }
}
