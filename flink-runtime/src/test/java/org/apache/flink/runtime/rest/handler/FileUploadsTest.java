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

package org.apache.flink.runtime.rest.handler;

import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.stream.Collectors;

/** Tests for {@link FileUploads}. */
public class FileUploadsTest extends TestLogger {

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testRelativePathRejection() throws IOException {
        Path relative = Paths.get("root");
        try {
            new FileUploads(relative);
            Assert.fail();
        } catch (IllegalArgumentException iae) {
            // expected
        }
    }

    @Test
    public void testDirectoryScan() throws IOException {
        Path rootDir = Paths.get("root");
        Path rootFile = rootDir.resolve("rootFile");
        Path subDir = rootDir.resolve("sub");
        Path subFile = subDir.resolve("subFile");

        Path tmp = temporaryFolder.getRoot().toPath();
        Files.createDirectory(tmp.resolve(rootDir));
        Files.createDirectory(tmp.resolve(subDir));
        Files.createFile(tmp.resolve(rootFile));
        Files.createFile(tmp.resolve(subFile));

        try (FileUploads fileUploads = new FileUploads(tmp.resolve(rootDir))) {
            Collection<Path> detectedFiles =
                    fileUploads.getUploadedFiles().stream()
                            .map(File::toPath)
                            .collect(Collectors.toList());

            Assert.assertEquals(2, detectedFiles.size());
            Assert.assertTrue(detectedFiles.contains(tmp.resolve(rootFile)));
            Assert.assertTrue(detectedFiles.contains(tmp.resolve(subFile)));
        }
    }

    @Test
    public void testEmptyDirectory() throws IOException {
        Path rootDir = Paths.get("root");

        Path tmp = temporaryFolder.getRoot().toPath();
        Files.createDirectory(tmp.resolve(rootDir));

        try (FileUploads fileUploads = new FileUploads(tmp.resolve(rootDir))) {
            Collection<File> detectedFiles = fileUploads.getUploadedFiles();
            Assert.assertEquals(0, detectedFiles.size());
        }
    }

    @Test
    public void testCleanup() throws IOException {
        Path rootDir = Paths.get("root");
        Path rootFile = rootDir.resolve("rootFile");
        Path subDir = rootDir.resolve("sub");
        Path subFile = subDir.resolve("subFile");

        Path tmp = temporaryFolder.getRoot().toPath();
        Files.createDirectory(tmp.resolve(rootDir));
        Files.createDirectory(tmp.resolve(subDir));
        Files.createFile(tmp.resolve(rootFile));
        Files.createFile(tmp.resolve(subFile));

        try (FileUploads fileUploads = new FileUploads(tmp.resolve(rootDir))) {
            Assert.assertTrue(Files.exists(tmp.resolve(rootDir)));
            Assert.assertTrue(Files.exists(tmp.resolve(subDir)));
            Assert.assertTrue(Files.exists(tmp.resolve(rootFile)));
            Assert.assertTrue(Files.exists(tmp.resolve(subFile)));
        }
        Assert.assertFalse(Files.exists(tmp.resolve(rootDir)));
        Assert.assertFalse(Files.exists(tmp.resolve(subDir)));
        Assert.assertFalse(Files.exists(tmp.resolve(rootFile)));
        Assert.assertFalse(Files.exists(tmp.resolve(subFile)));
    }
}
