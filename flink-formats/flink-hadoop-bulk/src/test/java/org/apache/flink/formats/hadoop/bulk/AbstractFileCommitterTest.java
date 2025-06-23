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

package org.apache.flink.formats.hadoop.bulk;

import org.apache.flink.util.IOUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests the behaviors of {@link HadoopFileCommitter}. */
public abstract class AbstractFileCommitterTest {

    private static final List<String> CONTENTS =
            new ArrayList<>(Arrays.asList("first line", "second line", "third line"));

    private final Configuration configuration;

    private final Path basePath;

    public AbstractFileCommitterTest() {
        this.configuration = getConfiguration();
        this.basePath = getBasePath();
    }

    @AfterEach
    void cleanup() throws IOException {
        cleanup(configuration, basePath);
    }

    protected abstract Configuration getConfiguration();

    protected abstract HadoopFileCommitter createNewCommitter(
            Configuration configuration, Path targetFilePath) throws IOException;

    protected abstract HadoopFileCommitter createPendingCommitter(
            Configuration configuration, Path targetFilePath, Path tempFilePath) throws IOException;

    protected abstract Path getBasePath();

    protected abstract void cleanup(Configuration configuration, Path basePath) throws IOException;

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testCommitOneFile(boolean override) throws IOException {
        Path targetFilePath = new Path(basePath, "part-0-0.txt");

        HadoopFileCommitter committer = createNewCommitter(configuration, targetFilePath);
        writeFile(committer.getTempFilePath(), configuration, override);

        committer.preCommit();
        verifyFileNotExists(configuration, basePath, "part-0-0.txt");

        committer.commit();
        verifyFolderAfterAllCommitted(configuration, basePath, "part-0-0.txt");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testCommitReWrittenFileAfterFailOver(boolean override) throws IOException {
        Path targetFilePath = new Path(basePath, "part-0-0.txt");

        HadoopFileCommitter committer = createNewCommitter(configuration, targetFilePath);
        writeFile(committer.getTempFilePath(), configuration, override);
        Path firstTempFilePath = committer.getTempFilePath();

        // Simulates restart the process and re-write the file.
        committer = createNewCommitter(configuration, targetFilePath);
        writeFile(committer.getTempFilePath(), configuration, override);

        committer.preCommit();
        verifyFileNotExists(configuration, basePath, "part-0-0.txt");

        committer.commit();
        verifyFolderAfterAllCommitted(
                configuration, basePath, "part-0-0.txt", firstTempFilePath.getName());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testCommitPreCommittedFileAfterFailOver(boolean override) throws IOException {
        Path targetFilePath = new Path(basePath, "part-0-0.txt");

        HadoopFileCommitter committer = createNewCommitter(configuration, targetFilePath);
        writeFile(committer.getTempFilePath(), configuration, override);

        committer.preCommit();
        verifyFileNotExists(configuration, basePath, "part-0-0.txt");

        // Simulates restart the process and continue committing the file.
        committer =
                createPendingCommitter(configuration, targetFilePath, committer.getTempFilePath());
        committer.commit();
        verifyFolderAfterAllCommitted(configuration, basePath, "part-0-0.txt");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testRepeatCommitAfterFailOver(boolean override) throws IOException {
        Path targetFilePath = new Path(basePath, "part-0-0.txt");

        HadoopFileCommitter committer = createNewCommitter(configuration, targetFilePath);
        writeFile(committer.getTempFilePath(), configuration, override);

        committer.preCommit();
        verifyFileNotExists(configuration, basePath, "part-0-0.txt");

        committer.commit();
        verifyFolderAfterAllCommitted(configuration, basePath, "part-0-0.txt");

        // Simulates restart the process and continue committing the file.
        committer =
                createPendingCommitter(configuration, targetFilePath, committer.getTempFilePath());
        committer.commitAfterRecovery();

        verifyFolderAfterAllCommitted(configuration, basePath, "part-0-0.txt");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testCommitMultipleFilesOneByOne(boolean override) throws IOException {
        Path targetFilePath1 = new Path(basePath, "part-0-0.txt");
        Path targetFilePath2 = new Path(basePath, "part-1-1.txt");

        HadoopFileCommitter committer1 = createNewCommitter(configuration, targetFilePath1);
        HadoopFileCommitter committer2 = createNewCommitter(configuration, targetFilePath2);

        writeFile(committer1.getTempFilePath(), configuration, override);
        writeFile(committer2.getTempFilePath(), configuration, override);

        committer1.preCommit();
        committer1.commit();

        verifyCommittedFiles(configuration, basePath, "part-0-0.txt");
        verifyFileNotExists(configuration, basePath, "part-1-1.txt");

        committer2.preCommit();
        committer2.commit();

        verifyFolderAfterAllCommitted(configuration, basePath, "part-0-0.txt", "part-1-1.txt");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testCommitMultipleFilesMixed(boolean override) throws IOException {
        Path targetFilePath1 = new Path(basePath, "part-0-0.txt");
        Path targetFilePath2 = new Path(basePath, "part-1-1.txt");

        HadoopFileCommitter committer1 = createNewCommitter(configuration, targetFilePath1);
        HadoopFileCommitter committer2 = createNewCommitter(configuration, targetFilePath2);

        writeFile(committer1.getTempFilePath(), configuration, override);
        writeFile(committer2.getTempFilePath(), configuration, override);

        committer1.preCommit();
        committer2.preCommit();

        verifyFileNotExists(configuration, basePath, "part-0-0.txt");
        verifyFileNotExists(configuration, basePath, "part-1-1.txt");

        committer1.commit();
        verifyCommittedFiles(configuration, basePath, "part-0-0.txt");
        verifyFileNotExists(configuration, basePath, "part-1-1.txt");

        committer2.commit();
        verifyFolderAfterAllCommitted(configuration, basePath, "part-0-0.txt", "part-1-1.txt");
    }

    // ---------------------------------------------------------------------------------------

    private void writeFile(Path path, Configuration configuration, boolean override)
            throws IOException {
        FileSystem fileSystem = FileSystem.get(path.toUri(), configuration);

        FSDataOutputStream fsDataOutputStream = null;
        PrintWriter printWriter = null;

        try {
            fsDataOutputStream = fileSystem.create(path, override);
            printWriter = new PrintWriter(fsDataOutputStream);

            for (String line : CONTENTS) {
                printWriter.println(line);
            }
        } finally {
            IOUtils.closeAllQuietly(printWriter, fsDataOutputStream);
        }
    }

    private void verifyFileNotExists(
            Configuration configuration, Path basePath, String... targetFileNames)
            throws IOException {

        FileSystem fileSystem = FileSystem.get(basePath.toUri(), configuration);
        for (String targetFileName : targetFileNames) {
            assertThat(fileSystem.exists(new Path(basePath, targetFileName)))
                    .as("Pre-committed file should not exist: " + targetFileName)
                    .isFalse();
        }
    }

    private void verifyCommittedFiles(
            Configuration configuration, Path basePath, String... targetFileNames)
            throws IOException {

        FileSystem fileSystem = FileSystem.get(basePath.toUri(), configuration);
        for (String targetFileName : targetFileNames) {
            Path targetFilePath = new Path(basePath, targetFileName);
            assertThat(fileSystem.exists(targetFilePath))
                    .as("Committed file should exist: " + targetFileName)
                    .isTrue();
            List<String> written = readFile(fileSystem, targetFilePath);
            assertThat(written)
                    .as("Unexpected file content for file " + targetFilePath)
                    .isEqualTo(CONTENTS);
        }
    }

    private void verifyFolderAfterAllCommitted(
            Configuration configuration, Path basePath, String... expectedFileNames)
            throws IOException {

        List<String> expectedNames = Arrays.asList(expectedFileNames);
        Collections.sort(expectedNames);

        FileSystem fileSystem = FileSystem.get(basePath.toUri(), configuration);
        FileStatus[] files = fileSystem.listStatus(basePath);
        List<String> fileNames = new ArrayList<>();
        for (FileStatus file : files) {
            fileNames.add(file.getPath().getName());
        }
        Collections.sort(fileNames);
        assertThat(fileNames).as("Remain files are " + fileNames).isEqualTo(expectedNames);

        for (FileStatus file : files) {
            if (!file.getPath().getName().startsWith(".")) {
                List<String> written = readFile(fileSystem, files[0].getPath());
                assertThat(written)
                        .as("Unexpected file content for file " + file.getPath())
                        .isEqualTo(CONTENTS);
            }
        }
    }

    private List<String> readFile(FileSystem fileSystem, Path partFile) throws IOException {
        try (FSDataInputStream dataInputStream = fileSystem.open(partFile)) {
            List<String> lines = new ArrayList<>();
            BufferedReader reader = new BufferedReader(new InputStreamReader(dataInputStream));
            String line = null;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }

            return lines;
        }
    }
}
