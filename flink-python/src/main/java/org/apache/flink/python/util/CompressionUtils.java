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

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

/**
 * Utils used to extract compressed files. It will try to restore the permission of the files if
 * possible.
 */
@Internal
public class CompressionUtils {

    private static final Logger LOG = LoggerFactory.getLogger(CompressionUtils.class);

    public static void extractFile(
            String srcFilePath, String targetDirPath, String originalFileName) throws IOException {
        if (hasOneOfSuffixes(originalFileName, ".zip", ".jar")) {
            extractZipFileWithPermissions(srcFilePath, targetDirPath);
        } else if (hasOneOfSuffixes(originalFileName, ".tar", ".tar.gz", ".tgz")) {
            extractTarFile(srcFilePath, targetDirPath);
        } else {
            LOG.warn(
                    "Only zip, jar, tar, tgz and tar.gz suffixes are supported, found {}. Trying to extract it as zip file.",
                    originalFileName);
            extractZipFileWithPermissions(srcFilePath, targetDirPath);
        }
    }

    public static void extractTarFile(String inFilePath, String targetDirPath) throws IOException {
        final File targetDir = new File(targetDirPath);
        if (!targetDir.mkdirs()) {
            if (!targetDir.isDirectory()) {
                throw new IOException("Mkdirs failed to create " + targetDir);
            }
        }
        final boolean gzipped = inFilePath.endsWith("gz");
        if (isUnix()) {
            extractTarFileUsingTar(inFilePath, targetDirPath, gzipped);
        } else {
            extractTarFileUsingJava(inFilePath, targetDirPath, gzipped);
        }
    }

    // Copy and simplify from hadoop-common package that is used in YARN
    // See
    // https://github.com/apache/hadoop/blob/7f93349ee74da5f35276b7535781714501ab2457/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/FileUtil.java
    private static void extractTarFileUsingTar(
            String inFilePath, String targetDirPath, boolean gzipped) throws IOException {
        inFilePath = makeSecureShellPath(inFilePath);
        targetDirPath = makeSecureShellPath(targetDirPath);
        String untarCommand =
                gzipped
                        ? String.format(
                                "gzip -dc '%s' | (cd '%s' && tar -xf -)", inFilePath, targetDirPath)
                        : String.format("cd '%s' && tar -xf '%s'", targetDirPath, inFilePath);
        Process process = new ProcessBuilder("bash", "-c", untarCommand).start();
        int exitCode = 0;
        try {
            exitCode = process.waitFor();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted when untarring file " + inFilePath);
        }
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
    private static void extractTarFileUsingJava(
            String inFilePath, String targetDirPath, boolean gzipped) throws IOException {
        try (InputStream fi = Files.newInputStream(Paths.get(inFilePath));
                InputStream bi = new BufferedInputStream(fi);
                final TarArchiveInputStream tai =
                        new TarArchiveInputStream(
                                gzipped ? new GzipCompressorInputStream(bi) : bi)) {
            final File targetDir = new File(targetDirPath);
            TarArchiveEntry entry;
            while ((entry = tai.getNextTarEntry()) != null) {
                unpackEntry(tai, entry, targetDir);
            }
        }
    }

    private static void unpackEntry(
            TarArchiveInputStream tis, TarArchiveEntry entry, File targetDir) throws IOException {
        String targetDirPath = targetDir.getCanonicalPath() + File.separator;
        File outputFile = new File(targetDir, entry.getName());
        if (!outputFile.getCanonicalPath().startsWith(targetDirPath)) {
            throw new IOException(
                    "expanding " + entry.getName() + " would create entry outside of " + targetDir);
        }

        if (entry.isDirectory()) {
            if (!outputFile.mkdirs() && !outputFile.isDirectory()) {
                throw new IOException("Failed to create directory " + outputFile);
            }

            for (TarArchiveEntry e : entry.getDirectoryEntries()) {
                unpackEntry(tis, e, outputFile);
            }

            return;
        }

        if (entry.isSymbolicLink()) {
            // create symbolic link relative to tar parent dir
            Files.createSymbolicLink(
                    Paths.get(new File(targetDir, entry.getName()).getCanonicalPath()),
                    Paths.get(entry.getLinkName()));
            return;
        }

        if (!outputFile.getParentFile().exists()) {
            if (!outputFile.getParentFile().mkdirs()) {
                throw new IOException("Mkdirs failed to create tar internal dir " + targetDir);
            }
        }

        try (OutputStream o = Files.newOutputStream(Paths.get(outputFile.getCanonicalPath()))) {
            IOUtils.copyBytes(tis, o, false);
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
                            // this may happens when the target file of the symlink is still not
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

    private static boolean hasOneOfSuffixes(String filePath, String... suffixes) {
        String lowercaseFilePath = filePath.toLowerCase();
        for (String suffix : suffixes) {
            if (lowercaseFilePath.endsWith(suffix)) {
                return true;
            }
        }
        return false;
    }
}
