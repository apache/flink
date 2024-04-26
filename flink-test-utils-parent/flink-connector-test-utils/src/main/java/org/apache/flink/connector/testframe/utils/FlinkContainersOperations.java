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

package org.apache.flink.connector.testframe.utils;

import org.apache.flink.connector.testframe.container.FlinkContainers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** A {@link FlinkContainers} docker containers related helper functions. */
public class FlinkContainersOperations {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkContainersOperations.class);
    private static final String NEWLINE = System.lineSeparator();

    private final FlinkContainers flink;

    /**
     * Creates an instance of {@link FlinkContainersOperations}.
     *
     * @param flink {@link FlinkContainers} docker based cluster
     */
    public FlinkContainersOperations(FlinkContainers flink) {
        this.flink = flink;
    }

    /**
     * Returns the content of files in the provided output path.
     *
     * <p>It expects that each line is terminated by line separator (e.g, '\n', '\r') or end-of-file
     * in the output file.
     *
     * @param outputPath path of output files
     * @param pattern file pattern to retrieve content
     * @param isSorted should the output be sorted
     * @return files content that match file pattern in the output path
     */
    public String getOutputFileContent(
            final String outputPath, final String pattern, final boolean isSorted)
            throws IOException {
        LOGGER.info(
                "Fetching content of files in '{}' path with pattern '{}'.", outputPath, pattern);
        final FileContentFetcher fileContentFetcher = new FileContentFetcher(outputPath, pattern);
        final StringBuilder sb = new StringBuilder();
        for (final GenericContainer<?> taskManager : flink.getTaskManagers()) {
            sb.append(fileContentFetcher.getFileContent(taskManager));
        }
        return isSorted ? sortLines(sb.toString()) : sb.toString();
    }

    private String sortLines(final String input) throws IOException {
        final BufferedReader bufferedReader = new BufferedReader(new StringReader(input));
        final List<String> lines = new ArrayList<>();
        String inputLine;
        while ((inputLine = bufferedReader.readLine()) != null) {
            lines.add(inputLine);
        }
        Collections.sort(lines);
        final StringBuilder sb = new StringBuilder();
        for (final String line : lines) {
            sb.append(line).append(NEWLINE);
        }
        return sb.toString();
    }

    private static class FileContentFetcher {
        private final String outputPath;
        private final String pattern;

        public FileContentFetcher(final String outputPath, final String pattern) {
            this.outputPath = outputPath;
            this.pattern = pattern;
        }

        public String getFileContent(final GenericContainer<?> container) {
            try {
                final Container.ExecResult result =
                        container.execInContainer(
                                "find",
                                this.outputPath,
                                "-name",
                                this.pattern,
                                "-exec",
                                "cat",
                                "{}",
                                "+");
                return result.getStdout();
            } catch (final InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        "Thread interrupted when reading file content", exception);
            } catch (final IOException exception) {
                throw new UncheckedIOException(
                        String.format(
                                "Exception reading content of file(s) on '%s' with pattern '%s'.",
                                this.outputPath, this.pattern),
                        exception);
            }
        }
    }
}
