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

package org.apache.flink.python.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.IOUtils;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Enumeration;

/** Utils used to extract zip files and try to restore the origin permissions of files. */
@Internal
public class ZipUtils {

    public static void extractZipFileWithPermissions(String zipFilePath, String targetPath)
            throws IOException {
        try (ZipFile zipFile = new ZipFile(zipFilePath)) {
            Enumeration<ZipArchiveEntry> entries = zipFile.getEntries();
            boolean isUnix = DecompressUtils.isUnix();

            while (entries.hasMoreElements()) {
                ZipArchiveEntry entry = entries.nextElement();
                File file;
                if (entry.isDirectory()) {
                    file = new File(targetPath, entry.getName());
                    if (!file.exists()) {
                        if (!file.mkdirs()) {
                            throw new IOException(
                                    "Create dir: " + file.getAbsolutePath() + " failed!");
                        }
                    }
                } else {
                    file = new File(targetPath, entry.getName());
                    File parentDir = file.getParentFile();
                    if (!parentDir.exists()) {
                        if (!parentDir.mkdirs()) {
                            throw new IOException(
                                    "Create dir: " + file.getAbsolutePath() + " failed!");
                        }
                    }
                    if (file.createNewFile()) {
                        OutputStream output = new FileOutputStream(file);
                        IOUtils.copyBytes(zipFile.getInputStream(entry), output);
                    } else {
                        throw new IOException(
                                "Create file: " + file.getAbsolutePath() + " failed!");
                    }
                }
                if (isUnix) {
                    int mode = entry.getUnixMode();
                    if (mode != 0) {
                        DecompressUtils.setFilePermission(file, mode);
                    }
                }
            }
        }
    }
}
