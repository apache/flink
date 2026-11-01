/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.client.program.artifact;

import org.apache.flink.util.FlinkRuntimeException;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Artifact fetch related utils. */
public class ArtifactUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ArtifactUtils.class);

    /**
     * Creates missing parent directories for the given {@link File} if there are any. Does nothing
     * otherwise.
     *
     * @param baseDir base dir to create parents for
     */
    public static synchronized void createMissingParents(File baseDir) {
        checkNotNull(baseDir, "Base dir has to be provided.");

        if (!baseDir.exists()) {
            try {
                FileUtils.forceMkdirParent(baseDir);
                LOG.info("Created parents for base dir: {}", baseDir);
            } catch (Exception e) {
                throw new FlinkRuntimeException(
                        String.format("Failed to create parent(s) for given base dir: %s", baseDir),
                        e);
            }
        }
    }

    /**
     * Artifacts are written to a temporary file in the same directory. Once the fetch is completed,
     * the file is moved to the target file location.
     *
     * <p>Fetches are skipped when an artifact is already in the target directory, which is why we
     * need to make sure that the target file location can't contain a partially fetched file.
     *
     * <p>Files for fetches-in-progress are given a {@code .part} file extension so they don't get
     * mistaken for artifacts, or cleaned up by another process sharing the same directory (e.g.
     * standby Job Manager).
     *
     * @param in the artifact, which the caller closes
     * @param targetFile where the complete artifact is put (this should not exist yet)
     */
    static void copyToFileWhenComplete(InputStream in, File targetFile) throws IOException {
        final File directory = targetFile.getAbsoluteFile().getParentFile();
        FileUtils.forceMkdir(directory);

        final File partFile =
                File.createTempFile(".fetch-" + targetFile.getName() + "-", ".part", directory);
        boolean moved = false;
        try {
            FileUtils.copyToFile(in, partFile);
            Files.move(partFile.toPath(), targetFile.toPath(), StandardCopyOption.ATOMIC_MOVE);
            moved = true;
        } finally {
            if (!moved) {
                FileUtils.deleteQuietly(partFile);
            }
        }
    }

    private ArtifactUtils() {
        throw new UnsupportedOperationException("This class should never be instantiated.");
    }
}
