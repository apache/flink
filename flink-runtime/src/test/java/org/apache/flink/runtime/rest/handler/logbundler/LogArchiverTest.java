/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.handler.logbundler;

import org.apache.flink.util.TestLogger;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;

/** Tests for {@link LogArchiver}. */
public class LogArchiverTest extends TestLogger {

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testLogArchiver() throws IOException {
        File archive = temporaryFolder.newFile();
        LogArchiver la = new LogArchiver(archive);

        la.addArchiveEntry("file", createFileContainingString("first"));
        la.addArchiveEntry("file", createFileContainingString("second"));
        la.addArchiveEntry("file", createFileContainingString("third"));

        la.closeArchive();

        assertThat(la.getArchive(), is(archive));

        List<String> filesInArchive = new ArrayList<>();
        try (InputStream bi = new FileInputStream(archive);
                InputStream gzi = new GzipCompressorInputStream(bi);
                ArchiveInputStream o = new TarArchiveInputStream(gzi)) {
            ArchiveEntry entry = null;
            while ((entry = o.getNextEntry()) != null) {
                filesInArchive.add(entry.getName());
                assertThat(entry.isDirectory(), is(false));
                assertThat(entry.getSize(), greaterThan(0L));
            }
        }
        assertThat(filesInArchive, hasSize(3));
        assertThat(filesInArchive, contains("file", "file.1", "file.2"));
    }

    @Test(expected = IllegalStateException.class)
    public void testGetWhenNotClosed() throws IOException {
        LogArchiver la = new LogArchiver(temporaryFolder.newFile());
        la.getArchive();
    }

    @Test(expected = IllegalStateException.class)
    public void testAddWhenClosed() throws IOException {
        LogArchiver la = new LogArchiver(temporaryFolder.newFile());
        la.closeArchive();
        la.addArchiveEntry("file", createFileContainingString("test"));
    }

    @Test
    public void testDestroyDeletesFile() throws IOException {
        File f = temporaryFolder.newFile();
        LogArchiver la = new LogArchiver(f);
        assertThat(f.exists(), is(true));
        la.destroy();
        assertThat(f.exists(), is(false));
    }

    private File createFileContainingString(String s) throws IOException {
        File f = temporaryFolder.newFile();
        Files.write(f.toPath(), s.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE);
        return f;
    }

    @Test
    public void testGetNextEntryName() {
        assertThat(
                LogArchiver.getNextEntryName("jobmanager-test.log"), is("jobmanager-test.log.1"));
        assertThat(
                LogArchiver.getNextEntryName("jobmanager-test.log.3"), is("jobmanager-test.log.4"));

        assertThat(
                LogArchiver.getNextEntryName("jobmanager-test.log.3.4.2.3"),
                is("jobmanager-test.log.3.4.2.4"));
    }

    @Test
    public void testCopy_full() throws IOException {
        byte[] input = new byte[10000];
        InputStream inputStream = new ByteArrayInputStream(input);
        ByteArrayOutputStream bao = new ByteArrayOutputStream();
        LogArchiver.copyWithLimit(inputStream, bao, input.length);

        assertThat(bao.size(), equalTo(input.length));
    }

    @Test
    public void testCopy_limited() throws IOException {
        int limit = 100;
        byte[] input = new byte[10000];
        InputStream inputStream = new ByteArrayInputStream(input);
        ByteArrayOutputStream bao = new ByteArrayOutputStream();
        LogArchiver.copyWithLimit(inputStream, bao, limit);

        assertThat(bao.size(), equalTo(limit));
    }

    @Test
    public void testCopy_limited2() throws IOException {
        int limit = 8100;
        byte[] input = new byte[10000];
        InputStream inputStream = new ByteArrayInputStream(input);
        ByteArrayOutputStream bao = new ByteArrayOutputStream();
        LogArchiver.copyWithLimit(inputStream, bao, limit);

        assertThat(bao.size(), equalTo(limit));
    }
}
