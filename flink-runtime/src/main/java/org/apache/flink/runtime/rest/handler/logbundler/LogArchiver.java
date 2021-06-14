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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger log = LoggerFactory.getLogger(LogArchiver.class);
    private static final Pattern namePattern = Pattern.compile("\\.([0-9]+)$");

    private final TarArchiveOutputStream archiveOutputStream;
    private final File archive;
    private boolean isClosed;
    private final Set<String> entryNames = new HashSet<>();

    public LogArchiver(File file) throws IOException {
        this.archive = file;

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
        return archive;
    }

    public synchronized void addArchiveEntry(
            String entryName, InputStream input, long size, long modTime) throws IOException {
        checkState(!isClosed, "Can not add archive entry to closed archiver");
        String internalEntryName = entryName;
        while (entryNames.contains(internalEntryName)) {
            internalEntryName = getNextEntryName(internalEntryName);
        }
        log.debug("Adding archive entry name:{} size:{}", internalEntryName, size);
        entryNames.add(internalEntryName);
        TarArchiveEntry entry = new TarArchiveEntry(internalEntryName);
        entry.setSize(size);
        entry.setMode(TarArchiveEntry.DEFAULT_FILE_MODE);
        entry.setModTime(modTime);
        entry.setUserName("");
        try {
            archiveOutputStream.putArchiveEntry(entry);
            // copy with limit: the input log file could have additional data, which would confuse
            // the archiver
            copyWithLimit(input, archiveOutputStream, size);
        } finally {
            archiveOutputStream.closeArchiveEntry();
        }
    }

    @VisibleForTesting
    static void copyWithLimit(final InputStream input, final OutputStream output, final long limit)
            throws IOException {
        byte[] buf = new byte[8192];
        long written = 0;
        int toRead;
        do {
            toRead = buf.length;
            if (written + toRead > limit) {
                toRead = (int) (limit - written);
                if (toRead == 0) {
                    break;
                }
            }
            input.read(buf, 0, toRead);
            output.write(buf, 0, toRead);
            written += toRead;
        } while (true);
    }

    public void addArchiveEntry(String entryName, File file) throws IOException {
        try (InputStream input = Files.newInputStream(file.toPath())) {
            addArchiveEntry(entryName, input, file.length(), file.lastModified());
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
        archive.delete();
    }

    public void closeArchive() throws IOException {
        if (isClosed) {
            return;
        }
        archiveOutputStream.close();
        isClosed = true;
    }
}
