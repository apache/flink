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
import org.apache.flink.core.fs.FileSystemBehaviorTestSuite;
import org.apache.flink.core.fs.FileSystemKind;
import org.apache.flink.core.fs.Path;
import org.apache.flink.testutils.cos.COSTestCredentials;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.UUID;

/** An implementation of the {@link FileSystemBehaviorTestSuite} for the COSN file system. */
public class HadoopCOSNFileSystemBehaviorITCase extends FileSystemBehaviorTestSuite {
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
    }

    @Override
    public FileSystem getFileSystem() throws Exception {
        return getBasePath().getFileSystem();
    }

    @Override
    public Path getBasePath() throws Exception {
        return new Path(COSTestCredentials.getTestBucketUri() + TEST_DATA_DIR);
    }

    @Override
    public FileSystemKind getFileSystemKind() {
        return FileSystemKind.OBJECT_STORE;
    }

    @AfterClass
    public static void clearFsConfig() throws IOException {
        FileSystem.initialize(new Configuration());
    }
}
