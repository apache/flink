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

package org.apache.flink.runtime.blob;

import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.util.Reference;
import org.apache.flink.util.TestLogger;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

/** Tests for {@link BlobUtils}. */
public class BlobUtilsTest extends TestLogger {

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    /**
     * Tests {@link BlobUtils#createBlobStorageDirectory} using {@link
     * BlobServerOptions#STORAGE_DIRECTORY} per default.
     */
    @Test
    public void testDefaultBlobStorageDirectory() throws IOException {
        Configuration config = new Configuration();
        String blobStorageDir = temporaryFolder.newFolder().getAbsolutePath();
        config.setString(BlobServerOptions.STORAGE_DIRECTORY, blobStorageDir);
        config.setString(CoreOptions.TMP_DIRS, temporaryFolder.newFolder().getAbsolutePath());

        File dir = BlobUtils.createBlobStorageDirectory(config, null).deref();
        assertThat(dir.getAbsolutePath(), startsWith(blobStorageDir));
    }

    /** Tests {@link BlobUtils#createBlobStorageDirectory}'s fallback to the fall back directory. */
    @Test
    public void testTaskManagerFallbackBlobStorageDirectory1() throws IOException {
        Configuration config = new Configuration();
        final File fallbackDirectory = new File(temporaryFolder.newFolder(), "foobar");

        File dir =
                BlobUtils.createBlobStorageDirectory(config, Reference.borrowed(fallbackDirectory))
                        .deref();
        assertThat(dir, is(equalTo(fallbackDirectory)));
    }

    @Test(expected = IOException.class)
    public void testBlobUtilsFailIfNoStorageDirectoryIsSpecified() throws IOException {
        BlobUtils.createBlobStorageDirectory(new Configuration(), null);
    }

    @Test
    public void testCheckAndDeleteCorruptedBlobsDeletesCorruptedBlobs() throws IOException {
        final File storageDir = temporaryFolder.newFolder();

        final BlobKey corruptedBlobKey = BlobKey.createKey(BlobKey.BlobType.PERMANENT_BLOB);
        final byte[] corruptedContent = "corrupted".getBytes(StandardCharsets.UTF_8);

        final byte[] validContent = "valid".getBytes(StandardCharsets.UTF_8);
        final byte[] validKey = BlobUtils.createMessageDigest().digest(validContent);
        final BlobKey validPermanentBlobKey =
                BlobKey.createKey(BlobKey.BlobType.PERMANENT_BLOB, validKey);
        final BlobKey validTransientBlobKey =
                BlobKey.createKey(BlobKey.BlobType.TRANSIENT_BLOB, validKey);

        writeBlob(storageDir, corruptedBlobKey, corruptedContent);
        writeBlob(storageDir, validPermanentBlobKey, validContent);
        writeBlob(storageDir, validTransientBlobKey, validContent);

        BlobUtils.checkAndDeleteCorruptedBlobs(storageDir.toPath(), log);

        assertThat(
                BlobUtils.listBlobsInDirectory(storageDir.toPath()).stream()
                        .map(BlobUtils.Blob::getBlobKey)
                        .collect(Collectors.toList()),
                containsInAnyOrder(validPermanentBlobKey, validTransientBlobKey));
    }

    private void writeBlob(File storageDir, BlobKey corruptedBlobKey, byte[] fileContent)
            throws IOException {
        final File corruptedFile = new File(storageDir, "corrupted");
        Files.write(corruptedFile.toPath(), fileContent);

        final File storageLocation =
                new File(
                        BlobUtils.getStorageLocationPath(
                                storageDir.getAbsolutePath(), null, corruptedBlobKey));
        FileUtils.createParentDirectories(storageLocation);
        Files.move(corruptedFile.toPath(), storageLocation.toPath());
    }
}
