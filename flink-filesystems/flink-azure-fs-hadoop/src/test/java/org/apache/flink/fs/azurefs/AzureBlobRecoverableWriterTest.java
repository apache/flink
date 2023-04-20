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

package org.apache.flink.fs.azurefs;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.AbstractRecoverableWriterTest;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.StringUtils;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.UUID;

/** Tests for the {@link AzureBlobRecoverableWriter}. */
public class AzureBlobRecoverableWriterTest extends AbstractRecoverableWriterTest {

    @ClassRule public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

    /** The cached file system instance. */
    private static FileSystem fileSystem;

    private static final String CONTAINER = System.getenv("ARTIFACTS_AZURE_CONTAINER");
    private static final String ACCOUNT = System.getenv("ARTIFACTS_AZURE_STORAGE_ACCOUNT");
    private static final String ACCESS_KEY = System.getenv("ARTIFACTS_AZURE_ACCESS_KEY");
    private static final String TEST_DATA_DIR = "tests-" + UUID.randomUUID();

    @BeforeClass
    public static void checkCredentialsAndSetup() throws IOException {
        // check whether credentials and container details exist
        Assume.assumeTrue(
                "Azure container not configured, skipping test...",
                !StringUtils.isNullOrWhitespaceOnly(CONTAINER));
        Assume.assumeTrue(
                "Azure access key not configured, skipping test...",
                !StringUtils.isNullOrWhitespaceOnly(ACCESS_KEY));
        // adjusting the minbuffer length for tests
        AzureBlobFsRecoverableDataOutputStream.minBufferLength = 4;
        // initialize configuration with valid credentials
        final Configuration conf = new Configuration();
        String uriString = "abfs://" + CONTAINER + "@" + ACCOUNT + ".dfs.core.windows.net";
        conf.setString("fs.defaultFS", uriString);
        conf.setString("fs.azure.account.auth.type", "SharedKey");
        conf.setString("fs.azure.account.key." + ACCOUNT + ".dfs.core.windows.net", ACCESS_KEY);
        // In 3.3.2 and above we try to create blocks locally. This fails if the runtime is not
        // able to find the scheme for 'file://'
        conf.setString("fs.azure.data.blocks.buffer", "bytebuffer");
        AbstractAzureFSFactory factory = new AzureDataLakeStoreGen2FSFactory();
        // abfs(s)://yourcontainer@youraccount.blob.core.windows.net/testDataDir
        Path path = new Path(uriString + "/" + TEST_DATA_DIR);
        factory.configure(conf);
        fileSystem = factory.create(path.toUri());

        FileSystem.initialize(conf);
    }

    @AfterClass
    public static void afterClass() {
        AzureBlobFsRecoverableDataOutputStream.minBufferLength = 2097152;
    }

    @Override
    public Path getBasePath() throws Exception {
        String uriString =
                "abfs"
                        + "://"
                        + CONTAINER
                        + '@'
                        + ACCOUNT
                        + ".blob.core.windows.net/"
                        + TEST_DATA_DIR;
        return new Path(uriString);
    }

    @Override
    public FileSystem initializeFileSystem() {
        return fileSystem;
    }
}
