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

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

@Internal
public class DecompressUtils {
    public static void unTar(String inFilePath, String targetDirPath)
            throws IOException, InterruptedException {
        File targetDir = new File(targetDirPath);
        if (!targetDir.mkdirs()) {
            if (!targetDir.isDirectory()) {
                throw new IOException("Mkdirs failed to create " + targetDir);
            }
        }
        boolean gzipped = inFilePath.endsWith("gz");
        if (isUnix()) {
            unTarUsingTar(inFilePath, targetDirPath, gzipped);
        } else {
            unTarUsingJava(inFilePath, targetDirPath, gzipped);
        }
    }

    // Copy and simplify from hadoop-common package that is used in YARN
    // See
    // https://github.com/apache/hadoop/blob/7f93349ee74da5f35276b7535781714501ab2457/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/FileUtil.java
    private static void unTarUsingTar(String inFilePath, String targetDirPath, boolean gzipped)
            throws IOException, InterruptedException {
        inFilePath = makeSecureShellPath(inFilePath);
        targetDirPath = makeSecureShellPath(targetDirPath);
        String untarCommand =
                gzipped
                        ? String.format(
                                "gzip -dc '%s' | (cd '%s' && tar -xf -)", inFilePath, targetDirPath)
                        : String.format("cd '%s' && tar -xf '%s'", targetDirPath, inFilePath);
        Process process = new ProcessBuilder("bash", "-c", untarCommand).start();
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new IOException(
                    "Error untarring file "
                            + inFilePath
                            + ". Tar process exited with exit code "
                            + exitCode);
        }
    }

    // Follow the pattern suggested in
    // https://commons.apache.org/proper/commons-compress/examples.html
    private static void unTarUsingJava(String inFilePath, String targetDirPath, boolean gzipped)
            throws IOException {
        try (InputStream fi = Files.newInputStream(Paths.get(inFilePath));
                InputStream bi = new BufferedInputStream(fi);
                ArchiveInputStream ai =
                        new TarArchiveInputStream(
                                gzipped ? new GzipCompressorInputStream(bi) : bi)) {
            ArchiveEntry entry;
            while ((entry = ai.getNextEntry()) != null) {
                File f = new File(targetDirPath, entry.getName());
                if (entry.isDirectory()) {
                    if (!f.isDirectory() && !f.mkdirs()) {
                        throw new IOException("Failed to create directory " + f);
                    }
                } else {
                    File parent = f.getParentFile();
                    if (!parent.isDirectory() && !parent.mkdirs()) {
                        throw new IOException("Failed to create directory " + parent);
                    }
                    try (OutputStream o = Files.newOutputStream(f.toPath())) {
                        IOUtils.copyBytes(ai, o, false);
                    }
                }
            }
        }
    }

    /**
     * Convert a os-native filename to a path that works for the shell and avoids script injection
     * attacks.
     */
    private static String makeSecureShellPath(String filePath) {
        return filePath.replace("'", "'\\''");
    }

    public static void extractZipFileWithPermissions(String zipFilePath, String targetPath)
            throws IOException {
        try (ZipFile zipFile = new ZipFile(zipFilePath)) {
            Enumeration<ZipArchiveEntry> entries = zipFile.getEntries();
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
                if (isUnix()) {
                    int mode = entry.getUnixMode();
                    if (mode != 0) {
                        Path path = Paths.get(file.toURI());
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
                        Files.setPosixFilePermissions(path, permissions);
                    }
                }
            }
        }
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

    private static boolean isUnix() {
        return OperatingSystem.isLinux()
                || OperatingSystem.isMac()
                || OperatingSystem.isFreeBSD()
                || OperatingSystem.isSolaris();
    }
}
