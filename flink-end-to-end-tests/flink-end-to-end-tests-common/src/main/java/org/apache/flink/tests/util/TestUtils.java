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

package org.apache.flink.tests.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

/** General test utilities. */
public enum TestUtils {
    ;

    /**
     * Copy all the files and sub-directories under source directory to destination directory
     * recursively.
     *
     * @param source directory or file path to copy from.
     * @param destination directory or file path to copy to.
     * @return Path of the destination directory.
     * @throws IOException if any IO error happen.
     */
    public static Path copyDirectory(final Path source, final Path destination) throws IOException {
        Files.walkFileTree(
                source,
                EnumSet.of(FileVisitOption.FOLLOW_LINKS),
                Integer.MAX_VALUE,
                new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes ignored)
                            throws IOException {
                        final Path targetDir = destination.resolve(source.relativize(dir));
                        try {
                            Files.copy(dir, targetDir, StandardCopyOption.COPY_ATTRIBUTES);
                        } catch (FileAlreadyExistsException e) {
                            if (!Files.isDirectory(targetDir)) {
                                throw e;
                            }
                        }
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes ignored)
                            throws IOException {
                        Files.copy(
                                file,
                                destination.resolve(source.relativize(file)),
                                StandardCopyOption.COPY_ATTRIBUTES);
                        return FileVisitResult.CONTINUE;
                    }
                });

        return destination;
    }

    /** Read the all files with the specified path. */
    public static List<String> readCsvResultFiles(Path path) throws IOException {
        File filePath = path.toFile();
        // list all the non-hidden files
        File[] csvFiles = filePath.listFiles((dir, name) -> !name.startsWith("."));
        List<String> result = new ArrayList<>();
        if (csvFiles != null) {
            for (File file : csvFiles) {
                result.addAll(Files.readAllLines(file.toPath()));
            }
        }
        return result;
    }
}
