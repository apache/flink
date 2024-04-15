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

    private ArtifactUtils() {
        throw new UnsupportedOperationException("This class should never be instantiated.");
    }
}
