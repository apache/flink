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

import org.apache.flink.annotation.VisibleForTesting;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.utils.IOUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkState;

/** Simple util for creating a log archive. */
public class LogArchiver {
    private static final Pattern namePattern = Pattern.compile("\\.([0-9]+)$");

    private final TarArchiveOutputStream archiveOutputStream;
    private final File file;
    private boolean isClosed;
    private final Set<String> entryNames = new HashSet<>();

    public LogArchiver(File file) throws IOException {
        this.file = file;
        file.deleteOnExit();

        OutputStream fo =
                Files.newOutputStream(
                        file.toPath(),
                        StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.TRUNCATE_EXISTING);
        OutputStream gzo = new GzipCompressorOutputStream(fo);
        this.archiveOutputStream = new TarArchiveOutputStream(gzo);
        isClosed = false;
        entryNames.clear();
    }

    public File getArchive() {
        checkState(isClosed, "Can not download archive before it has been closed.");
        return file;
    }

    public synchronized void addArchiveEntry(String entryName, InputStream input, long size)
            throws IOException {
        checkState(!isClosed, "Can not add archive entry to closed archiver");
        String internalEntryName = entryName;
        while (entryNames.contains(internalEntryName)) {
            internalEntryName = getNextEntryName(internalEntryName);
        }
        entryNames.add(internalEntryName);
        // ArchiveEntry entry = archiveOutputStream.createArchiveEntry(file, internalEntryName);
        TarArchiveEntry entry = new TarArchiveEntry(internalEntryName);
        entry.setSize(size);
        entry.setMode(TarArchiveEntry.DEFAULT_FILE_MODE);
        entry.setModTime(file.lastModified() / TarArchiveEntry.MILLIS_PER_SECOND);
        entry.setUserName("");
        archiveOutputStream.putArchiveEntry(entry);
        IOUtils.copy(input, archiveOutputStream);
        archiveOutputStream.closeArchiveEntry();
    }

    public void addArchiveEntry(String entryName, File file) throws IOException {
        try (InputStream input = Files.newInputStream(file.toPath())) {
            addArchiveEntry(entryName, input, file.length());
        }
    }

    @VisibleForTesting
    static String getNextEntryName(String name) {
        Matcher matcher = namePattern.matcher(name);
        if (!matcher.find()) {
            return name + ".1";
        } else {
            String number = matcher.group(1);
            int newNumber = Integer.parseInt(number) + 1;
            return matcher.replaceAll("." + newNumber);
        }
    }

    public void destroy() throws IOException {
        closeArchive();
        file.delete();
    }

    public void closeArchive() throws IOException {
        if (isClosed) {
            return;
        }
        archiveOutputStream.close();
        isClosed = true;
    }
}
