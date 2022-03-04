/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.util;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;

/**
 * Util class for implementing common functions in junit4 {@link org.junit.rules.TemporaryFolder},
 * like newFolder() and newFile() for junit5 {@link org.junit.jupiter.api.io.TempDir}.
 */
public class TempDirUtils {

    private TempDirUtils() {}

    public static Path newFolderIn(@Nonnull Path baseTempPath) {
        Path newFolder = baseTempPath.resolve(UUID.randomUUID().toString());
        newFolder.toFile().mkdir();
        return newFolder;
    }

    public static Path newFileIn(@Nonnull Path baseTempPath) throws IOException {
        Path filePath = baseTempPath.resolve(UUID.randomUUID().toString());
        filePath.toFile().createNewFile();
        return filePath;
    }

    public static File newFolderIn(@Nonnull File baseTempDirFile) {
        Path newFolder = baseTempDirFile.toPath().resolve(UUID.randomUUID().toString());
        newFolder.toFile().mkdir();
        return newFolder.toFile();
    }

    public static File newFileIn(@Nonnull File baseTempDirFile) throws IOException {
        Path newFolder = baseTempDirFile.toPath().resolve(UUID.randomUUID().toString());
        newFolder.toFile().createNewFile();
        return newFolder.toFile();
    }

    public static Path newFolderIn(@Nonnull Path baseTempDir, String name) {
        Path p = baseTempDir.resolve(name);
        p.toFile().mkdir();
        return p;
    }

    public static File newFolderIn(@Nonnull File baseTempDir, String name) {
        Path p = baseTempDir.toPath().resolve(name);
        p.toFile().mkdir();
        return p.toFile();
    }

    public static Path newFileIn(@Nonnull Path baseTempDir, String name) throws IOException {
        Path p = baseTempDir.resolve(name);
        p.toFile().createNewFile();
        return p;
    }

    public static File newFileIn(@Nonnull File baseTempDir, String name) throws IOException {
        Path p = baseTempDir.toPath().resolve(name);
        p.toFile().createNewFile();
        return p.toFile();
    }
}
