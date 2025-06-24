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
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemBehaviorTestSuite;
import org.apache.flink.core.fs.FileSystemKind;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.StringUtils;

import com.microsoft.azure.credentials.ApplicationTokenCredentials;
import com.microsoft.azure.credentials.AzureTokenCredentials;
import com.microsoft.azure.management.Azure;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.apache.flink.core.fs.FileSystemTestUtils.checkPathEventualExistence;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/** An implementation of the {@link FileSystemBehaviorTestSuite} for Azure based file system. */
class AzureFileSystemBehaviorITCase extends FileSystemBehaviorTestSuite {

    private static final String CONTAINER = System.getenv("ARTIFACTS_AZURE_CONTAINER");
    private static final String ACCOUNT = System.getenv("ARTIFACTS_AZURE_STORAGE_ACCOUNT");
    private static final String ACCESS_KEY = System.getenv("ARTIFACTS_AZURE_ACCESS_KEY");
    private static final String RESOURCE_GROUP = System.getenv("ARTIFACTS_AZURE_RESOURCE_GROUP");
    private static final String SUBSCRIPTION_ID = System.getenv("ARTIFACTS_AZURE_SUBSCRIPTION_ID");
    private static final String TOKEN_CREDENTIALS_FILE =
            System.getenv("ARTIFACTS_AZURE_TOKEN_CREDENTIALS_FILE");

    private static final String TEST_DATA_DIR = "tests-" + UUID.randomUUID();

    /**
     * Azure Blob Storage defaults to https only storage accounts, tested in the base class.
     *
     * <p>This nested class repeats the tests with http support, but only if a best effort check on
     * https support succeeds.
     */
    static class HttpSupportAzureFileSystemBehaviorITCase extends AzureFileSystemBehaviorITCase {
        @BeforeAll
        static void onlyRunIfHttps() throws IOException {
            // default to https only, as some fields are missing
            assumeThat(RESOURCE_GROUP)
                    .describedAs("Azure resource group not configured, skipping test...")
                    .isNotBlank();
            assumeThat(TOKEN_CREDENTIALS_FILE)
                    .describedAs("Azure token credentials not configured, skipping test...")
                    .isNotBlank();
            assumeThat(isHttpsTrafficOnly()).isFalse();
        }

        @Override
        protected Path getBasePath() {
            // wasb://yourcontainer@youraccount.blob.core.windows.net/testDataDir
            String uriString =
                    "wasb://"
                            + CONTAINER
                            + '@'
                            + ACCOUNT
                            + ".blob.core.windows.net/"
                            + TEST_DATA_DIR;
            return new Path(uriString);
        }
    }

    private static boolean isHttpsTrafficOnly() throws IOException {
        AzureTokenCredentials credentials =
                ApplicationTokenCredentials.fromFile(new File(TOKEN_CREDENTIALS_FILE));
        Azure azure =
                StringUtils.isNullOrWhitespaceOnly(SUBSCRIPTION_ID)
                        ? Azure.authenticate(credentials).withDefaultSubscription()
                        : Azure.authenticate(credentials).withSubscription(SUBSCRIPTION_ID);

        return azure.storageAccounts()
                .getByResourceGroup(RESOURCE_GROUP, ACCOUNT)
                .inner()
                .enableHttpsTrafficOnly();
    }

    @BeforeAll
    static void checkCredentialsAndSetup() {
        // check whether credentials and container details exist
        assumeThat(ACCOUNT)
                .describedAs("Azure storage account not configured, skipping test...")
                .isNotBlank();
        assumeThat(CONTAINER)
                .describedAs("Azure container not configured, skipping test...")
                .isNotBlank();
        assumeThat(ACCESS_KEY)
                .describedAs("Azure access key not configured, skipping test...")
                .isNotBlank();

        // initialize configuration with valid credentials
        final Configuration conf = new Configuration();
        // fs.azure.account.key.youraccount.blob.core.windows.net = ACCESS_KEY
        conf.setString("fs.azure.account.key." + ACCOUNT + ".blob.core.windows.net", ACCESS_KEY);
        FileSystem.initialize(conf, null);
    }

    @AfterAll
    static void clearFsConfig() {
        FileSystem.initialize(new Configuration(), null);
    }

    @Override
    protected FileSystem getFileSystem() throws Exception {
        return getBasePath().getFileSystem();
    }

    @Override
    protected Path getBasePath() {
        // wasbs://yourcontainer@youraccount.blob.core.windows.net/testDataDir
        String uriString =
                "wasbs://" + CONTAINER + '@' + ACCOUNT + ".blob.core.windows.net/" + TEST_DATA_DIR;
        return new Path(uriString);
    }

    @Override
    protected FileSystemKind getFileSystemKind() {
        return FileSystemKind.OBJECT_STORE;
    }

    @Test
    void testSimpleFileWriteAndRead() throws Exception {
        final long deadline = System.nanoTime() + 30_000_000_000L; // 30 secs

        final String testLine = "Hello Upload!";

        final Path path = new Path(getBasePath() + "/test.txt");
        final FileSystem fs = path.getFileSystem();

        try {
            try (FSDataOutputStream out = fs.create(path, FileSystem.WriteMode.OVERWRITE);
                    OutputStreamWriter writer =
                            new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
                writer.write(testLine);
            }

            // just in case, wait for the path to exist
            checkPathEventualExistence(fs, path, true, deadline);

            try (FSDataInputStream in = fs.open(path);
                    InputStreamReader ir = new InputStreamReader(in, StandardCharsets.UTF_8);
                    BufferedReader reader = new BufferedReader(ir)) {
                String line = reader.readLine();
                assertThat(line).isEqualTo(testLine);
            }
        } finally {
            fs.delete(path, false);
        }

        // now file must be gone
        checkPathEventualExistence(fs, path, false, deadline);
    }

    @Test
    void testDirectoryListing() throws Exception {
        final long deadline = System.nanoTime() + 30_000_000_000L; // 30 secs

        final Path directory = new Path(getBasePath() + "/testdir/");
        final FileSystem fs = directory.getFileSystem();

        // directory must not yet exist
        assertThat(fs.exists(directory)).isFalse();

        try {
            // create directory
            assertThat(fs.mkdirs(directory)).isTrue();

            checkPathEventualExistence(fs, directory, true, deadline);

            // directory empty
            assertThat(fs.listStatus(directory)).isEmpty();

            // create some files
            final int numFiles = 3;
            for (int i = 0; i < numFiles; i++) {
                Path file = new Path(directory, "/file-" + i);
                try (FSDataOutputStream out = fs.create(file, FileSystem.WriteMode.OVERWRITE);
                        OutputStreamWriter writer =
                                new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
                    writer.write("hello-" + i + "\n");
                }
                // just in case, wait for the file to exist (should then also be reflected in the
                // directory's file list below)
                checkPathEventualExistence(fs, file, true, deadline);
            }

            FileStatus[] files = fs.listStatus(directory);
            assertThat(files).hasSize(3);

            for (FileStatus status : files) {
                assertThat(status.isDir()).isFalse();
            }

            // now that there are files, the directory must exist
            assertThat(fs.exists(directory)).isTrue();
        } finally {
            // clean up
            fs.delete(directory, true);
        }

        // now directory must be gone
        checkPathEventualExistence(fs, directory, false, deadline);
    }
}
