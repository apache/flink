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

package org.apache.flink.core.fs;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkState;

/** Tests for the {@link RefCountedFile}. */
public class RefCountedFileTest {

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void releaseToZeroRefCounterShouldDeleteTheFile() throws IOException {
        final File newFile = new File(temporaryFolder.getRoot(), ".tmp_" + UUID.randomUUID());
        checkState(newFile.createNewFile());

        RefCountedFile fileUnderTest = new RefCountedFile(newFile);
        verifyTheFileIsStillThere();

        fileUnderTest.release();

        try (Stream<Path> files = Files.list(temporaryFolder.getRoot().toPath())) {
            Assert.assertEquals(0L, files.count());
        }
    }

    @Test
    public void retainsShouldRequirePlusOneReleasesToDeleteTheFile() throws IOException {
        final File newFile = new File(temporaryFolder.getRoot(), ".tmp_" + UUID.randomUUID());
        checkState(newFile.createNewFile());

        // the reference counter always starts with 1 (not 0). This is why we need +1 releases
        RefCountedFile fileUnderTest = new RefCountedFile(newFile);
        verifyTheFileIsStillThere();

        fileUnderTest.retain();
        fileUnderTest.retain();

        Assert.assertEquals(3, fileUnderTest.getReferenceCounter());

        fileUnderTest.release();
        Assert.assertEquals(2, fileUnderTest.getReferenceCounter());
        verifyTheFileIsStillThere();

        fileUnderTest.release();
        Assert.assertEquals(1, fileUnderTest.getReferenceCounter());
        verifyTheFileIsStillThere();

        fileUnderTest.release();
        // the file is deleted now
        try (Stream<Path> files = Files.list(temporaryFolder.getRoot().toPath())) {
            Assert.assertEquals(0L, files.count());
        }
    }

    private void verifyTheFileIsStillThere() throws IOException {
        try (Stream<Path> files = Files.list(temporaryFolder.getRoot().toPath())) {
            Assert.assertEquals(1L, files.count());
        }
    }

    private static byte[] bytesOf(String str) {
        return str.getBytes(StandardCharsets.UTF_8);
    }
}
