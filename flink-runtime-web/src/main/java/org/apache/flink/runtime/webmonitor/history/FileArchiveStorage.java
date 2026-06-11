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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.webmonitor.history;

import org.apache.flink.util.FileUtils;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A file-system backed implementation of {@link ArchiveStorage}.
 *
 * <p>Each storage key is treated as a relative path under the configured {@code rootPath}.
 */
public class FileArchiveStorage implements ArchiveStorage<File> {

    /** Root directory under which all archive files are stored. */
    private final Path rootPath;

    /**
     * Creates a new {@link FileArchiveStorage}.
     *
     * @param rootPath root directory for archive files; must not be {@code null}
     * @throws IOException if the canonical path of {@code rootPath} cannot be resolved
     */
    public FileArchiveStorage(File rootPath) throws IOException {
        this.rootPath = checkNotNull(rootPath).getCanonicalFile().toPath();
    }

    @Override
    public boolean exists(String key) throws IOException {
        return Files.exists(rootPath.resolve(key));
    }

    @Nullable
    @Override
    public File getEntry(String key) throws IOException {
        if (!exists(key)) {
            return null;
        }
        return rootPath.resolve(key).toFile();
    }

    @Override
    public void putArchiveContent(String key, String archiveContent) throws IOException {
        writeTargetFile(rootPath.resolve(key), archiveContent);
    }

    @Override
    public void delete(String key) throws IOException {
        FileUtils.deleteFileOrDirectory(rootPath.resolve(key).toFile());
    }

    @Override
    public void deleteEntriesByPrefix(String keyPrefix) throws IOException {
        FileUtils.deleteFileOrDirectory(rootPath.resolve(keyPrefix).toFile());
    }

    @Override
    public List<File> getEntriesByPrefix(String keyPrefix) throws IOException {
        Path directory = rootPath.resolve(keyPrefix);
        if (!Files.isDirectory(directory)) {
            return new ArrayList<>();
        }
        try (Stream<Path> entries = Files.list(directory)) {
            return entries.map(Path::toFile).collect(Collectors.toList());
        }
    }

    @Override
    public String readArchiveContent(File file) throws IOException {
        return FileUtils.readFileUtf8(file);
    }

    void writeTargetFile(Path target, String json) throws IOException {
        Path parent = target.getParent();

        try {
            Files.createDirectories(parent);
        } catch (FileAlreadyExistsException ignored) {
            // there may be left-over directories from the previous attempt
        }

        // We overwrite existing files since this may be another attempt
        // at fetching this archive.
        // Existing files may be incomplete/corrupt.
        Files.deleteIfExists(target);

        Files.write(target, json.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void close() throws IOException {
        // Nothing to close.
    }
}
