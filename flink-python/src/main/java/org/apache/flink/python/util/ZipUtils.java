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
import org.apache.flink.util.OperatingSystem;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

/** Utils used to extract zip files and try to restore the origin permissions of files. */
@Internal
public final class ZipUtils {

    public static void extractZipFileWithPermissions(String zipFilePath, String targetPath)
            throws IOException {
        try (ZipFile zipFile = new ZipFile(zipFilePath)) {
            Enumeration<ZipArchiveEntry> entries = zipFile.getEntries();
            boolean isUnix = isUnix();
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            String canonicalTargetPath = new File(targetPath).getCanonicalPath() + File.separator;

            while (entries.hasMoreElements()) {
                ZipArchiveEntry entry = entries.nextElement();
                File outputFile = new File(canonicalTargetPath, entry.getName());
                if (!outputFile.getCanonicalPath().startsWith(canonicalTargetPath)) {
                    throw new IOException(
                            "Expand "
                                    + entry.getName()
                                    + " would create a file outside of "
                                    + targetPath);
                }

                if (entry.isDirectory()) {
                    if (!outputFile.exists()) {
                        if (!outputFile.mkdirs()) {
                            throw new IOException(
                                    "Create dir: " + outputFile.getAbsolutePath() + " failed!");
                        }
                    }
                } else {
                    File parentDir = outputFile.getParentFile();
                    if (!parentDir.exists()) {
                        if (!parentDir.mkdirs()) {
                            throw new IOException(
                                    "Create dir: " + outputFile.getAbsolutePath() + " failed!");
                        }
                    }
                    if (entry.isUnixSymlink()) {
                        // the content of the file is the target path of the symlink
                        baos.reset();
                        IOUtils.copyBytes(zipFile.getInputStream(entry), baos);
                        Files.createSymbolicLink(
                                outputFile.toPath(), new File(parentDir, baos.toString()).toPath());
                    } else if (outputFile.createNewFile()) {
                        OutputStream output = new FileOutputStream(outputFile);
                        IOUtils.copyBytes(zipFile.getInputStream(entry), output);
                    } else {
                        throw new IOException(
                                "Create file: " + outputFile.getAbsolutePath() + " failed!");
                    }
                }
                if (isUnix) {
                    int mode = entry.getUnixMode();
                    if (mode != 0) {
                        Path outputPath = Paths.get(outputFile.toURI());
                        Set<PosixFilePermission> permissions = new HashSet<>();
                        addIfBitSet(mode, 8, permissions, PosixFilePermission.OWNER_READ);
                        addIfBitSet(mode, 7, permissions, PosixFilePermission.OWNER_WRITE);
                        addIfBitSet(mode, 6, permissions, PosixFilePermission.OWNER_EXECUTE);
                        addIfBitSet(mode, 5, permissions, PosixFilePermission.GROUP_READ);
                        addIfBitSet(mode, 4, permissions, PosixFilePermission.GROUP_WRITE);
                        addIfBitSet(mode, 3, permissions, PosixFilePermission.GROUP_EXECUTE);
                        addIfBitSet(mode, 2, permissions, PosixFilePermission.OTHERS_READ);
                        addIfBitSet(mode, 1, permissions, PosixFilePermission.OTHERS_WRITE);
                        addIfBitSet(mode, 0, permissions, PosixFilePermission.OTHERS_EXECUTE);
                        // the permission of the target file will be set to be the same as the
                        // symlink
                        // TODO: support setting the permission without following links
                        try {
                            Files.setPosixFilePermissions(outputPath, permissions);
                        } catch (NoSuchFileException e) {
                            // this may happen when the target file of the symlink is still not
                            // extracted
                        }
                    }
                }
            }
        }
    }

    private static boolean isUnix() {
        return OperatingSystem.isLinux()
                || OperatingSystem.isMac()
                || OperatingSystem.isFreeBSD()
                || OperatingSystem.isSolaris();
    }

    private static void addIfBitSet(
            int mode,
            int pos,
            Set<PosixFilePermission> posixFilePermissions,
            PosixFilePermission posixFilePermissionToAdd) {
        if ((mode & 1L << pos) != 0L) {
            posixFilePermissions.add(posixFilePermissionToAdd);
        }
    }
}
