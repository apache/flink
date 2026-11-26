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

package org.apache.flink.client.cli;

import org.apache.flink.client.program.artifact.ArtifactFetchManager;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipal;
import java.security.MessageDigest;
import java.util.Optional;

import static java.nio.file.LinkOption.NOFOLLOW_LINKS;

/**
 * Job JARs that the CLI fetched from a file system URI ({@code s3://}, {@code hdfs://}, ...), kept
 * under {@code io.tmp.dirs} so that submitting several jobs from the same JAR downloads it once. A
 * cached copy is reused only while the remote file keeps its length and modification time.
 */
final class JobJarCache {

    static final String DIRECTORY_NAME = "flink-cli-jar-cache";

    private static final String STATUS_FILE = ".status";

    private static final Logger LOG = LoggerFactory.getLogger(JobJarCache.class);

    private final Path root;

    private JobJarCache(Path root) {
        this.root = root;
    }

    /**
     * Opens the cache under the given directory. Returns empty if the cache directory is not a
     * directory that only the current user can access: the CLI runs the cached code, so a directory
     * others can write to (e.g. a shared {@code /tmp}) must not be trusted.
     */
    static Optional<JobJarCache> open(String tmpDir) {
        Path root = Paths.get(tmpDir, DIRECTORY_NAME);
        try {
            if (!Files.exists(root, NOFOLLOW_LINKS)) {
                try {
                    Files.createDirectory(
                            root,
                            PosixFilePermissions.asFileAttribute(
                                    PosixFilePermissions.fromString("rwx------")));
                } catch (FileAlreadyExistsException e) {
                    // Created concurrently; checked below like any existing directory.
                }
            }
            if (isPrivate(root)) {
                return Optional.of(new JobJarCache(root));
            }
            LOG.warn(
                    "Not caching job JARs: {} must be a directory owned by and accessible only to"
                            + " the current user.",
                    root);
        } catch (IOException | UnsupportedOperationException e) {
            LOG.info("Not caching job JARs under {}: {}", root, e.toString());
        }
        return Optional.empty();
    }

    private static boolean isPrivate(Path dir) throws IOException {
        if (!Files.isDirectory(dir, NOFOLLOW_LINKS)) {
            return false;
        }
        PosixFileAttributes attributes =
                Files.readAttributes(dir, PosixFileAttributes.class, NOFOLLOW_LINKS);
        UserPrincipal currentUser =
                dir.getFileSystem()
                        .getUserPrincipalLookupService()
                        .lookupPrincipalByName(System.getProperty("user.name"));
        return attributes.owner().equals(currentUser)
                && attributes.permissions().stream()
                        .allMatch(
                                permission ->
                                        permission == PosixFilePermission.OWNER_READ
                                                || permission == PosixFilePermission.OWNER_WRITE
                                                || permission == PosixFilePermission.OWNER_EXECUTE);
    }

    /** Returns the cached copy of the JAR, fetching it first if missing or changed remotely. */
    File get(String jarUri, Configuration configuration) throws Exception {
        org.apache.flink.core.fs.Path remote = new org.apache.flink.core.fs.Path(jarUri);
        FileStatus remoteStatus = remote.getFileSystem().getFileStatus(remote);
        String stamp = remoteStatus.getLen() + " " + remoteStatus.getModificationTime();

        Path entry = Files.createDirectories(root.resolve(sha256Hex(jarUri)));
        Path jar = entry.resolve(remote.getName());
        Path statusFile = entry.resolve(STATUS_FILE);
        if (Files.isRegularFile(jar, NOFOLLOW_LINKS)
                && Files.isRegularFile(statusFile, NOFOLLOW_LINKS)
                && stamp.equals(
                        new String(Files.readAllBytes(statusFile), StandardCharsets.UTF_8))) {
            LOG.info("Reusing cached job JAR {} for {}.", jar, jarUri);
            return jar.toFile();
        }

        Path download = Files.createTempDirectory(entry, "fetch-");
        try {
            File fetched =
                    new ArtifactFetchManager(configuration, download.toString())
                            .fetchArtifacts(jarUri, null)
                            .getJobJar();
            Path newStatus =
                    Files.write(
                            download.resolve(STATUS_FILE), stamp.getBytes(StandardCharsets.UTF_8));
            // Moved one at a time, so a concurrent CLI may briefly see the new JAR with the old
            // status. The old status no longer matches the remote file, so it fetches again
            // instead of running a JAR that does not match its status.
            Files.move(fetched.toPath(), jar, StandardCopyOption.ATOMIC_MOVE);
            Files.move(newStatus, statusFile, StandardCopyOption.ATOMIC_MOVE);
            LOG.info("Fetched job JAR {} to {}.", jarUri, jar);
            return jar.toFile();
        } finally {
            FileUtils.deleteDirectoryQuietly(download.toFile());
        }
    }

    private static String sha256Hex(String value) throws Exception {
        return StringUtils.byteToHexString(
                MessageDigest.getInstance("SHA-256")
                        .digest(value.getBytes(StandardCharsets.UTF_8)));
    }
}
