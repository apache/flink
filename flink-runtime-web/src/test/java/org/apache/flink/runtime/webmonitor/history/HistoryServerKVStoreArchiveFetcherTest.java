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

package org.apache.flink.runtime.webmonitor.history;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.webmonitor.history.kvstore.KVStore;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.RocksDB;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.apache.flink.runtime.webmonitor.history.HistoryServerKVStoreArchiveFetcher.updateJobOverview;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for the HistoryServerKVStoreArchiveFetcher class. */
public class HistoryServerKVStoreArchiveFetcherTest {

    private HistoryServerKVStoreArchiveFetcher archiveFetcher;
    private List<HistoryServer.RefreshLocation> refreshDirs;

    private KVStore<String, String> kvStore;

    @TempDir java.nio.file.Path tmpDir;

    @BeforeEach
    public void setUp() throws Exception {
        RocksDB.loadLibrary();

        // Create RocksDB path
        java.nio.file.Path rocksDBPath = tmpDir.resolve("rocksdb");
        File rocksDBFile = rocksDBPath.toFile();

        // Create a temporary directory for archives
        java.nio.file.Path tempDir = Files.createDirectory(tmpDir.resolve("tmpDir"));
        Path tempDirFs = new Path(tempDir.toUri());
        FileSystem refreshFS = tempDirFs.getFileSystem();

        // Initialize refreshDirs with the temporary directory
        refreshDirs = new ArrayList<>();
        refreshDirs.add(new HistoryServer.RefreshLocation(tempDirFs, refreshFS));

        // Initialize a dummy event listener for job archive events
        Consumer<ArchiveFetcher.ArchiveEvent> jobArchiveEventListener = event -> {};

        archiveFetcher =
                new HistoryServerKVStoreArchiveFetcher(
                        refreshDirs, // List<HistoryServer.RefreshLocation>
                        rocksDBFile, // RocksDB Path
                        jobArchiveEventListener, // Consumer<ArchiveEvent>
                        true, // cleanupExpiredArchives
                        10); // maxHistorySize

        kvStore = archiveFetcher.getKVStore();
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (archiveFetcher != null) {
            kvStore.close();
        }
        deleteDirectory(tmpDir.toFile());
    }

    private static void deleteDirectory(File directory) throws IOException {
        if (directory.exists()) {
            Files.walk(directory.toPath()).map(java.nio.file.Path::toFile).forEach(File::delete);
        }
    }

    @Test
    void testProcessOneArchive() throws Exception {
        String jobID = "simpleJobID";
        java.nio.file.Path jobArchive = createOneMockArchive(jobID);

        // Process the archive
        archiveFetcher.processArchive(jobID, new Path(jobArchive.toUri()));

        // Verify the data in RocksDB
        String fetchedEnvJson = kvStore.get("/jobs/" + jobID + "/jobmanager/environment");
        assertThat(fetchedEnvJson)
                .isEqualTo(
                        "{\"jvm\":{\"version\":\"Java HotSpot(TM) 64-Bit Server VM\",\"arch\":\"amd64\",\"options\":[\"-Xmx1024m\",\"-Xms1024m\"]}}");

        String fetchedOverviewJson = kvStore.get("/jobs/overview/" + jobID);
        assertThat(fetchedOverviewJson)
                .isEqualTo(
                        "{\"jobs\":[{\"jid\":\"simpleJobID\",\"name\":\"Flink Streaming Job\",\"state\":\"FINISHED\",\"start-time\":1723270676500,\"end-time\":1723270676621,\"duration\":121,\"last-modification\":1723270676621,\"tasks\":{\"total\":3,\"created\":0,\"scheduled\":0,\"deploying\":0,\"running\":0,\"finished\":3,\"canceling\":0,\"canceled\":0,\"failed\":0,\"reconciling\":0,\"initializing\":0}}]}");

        String fetchedConfigJson = kvStore.get("/jobs/" + jobID + "/config");
        assertThat(fetchedConfigJson)
                .isEqualTo(
                        "{\"jid\":\"simpleJobID\",\"name\":\"Example Job\",\"execution-config\":{\"execution-mode\":\"PIPELINED\",\"restart-strategy\":\"Restart with fixed delay\",\"job-parallelism\":1}}");

        String fetchedCheckpointConfigJson = kvStore.get("/jobs/" + jobID + "/checkpoints/config");
        assertThat(fetchedCheckpointConfigJson)
                .isEqualTo("{\"errors\":[\"Checkpointing is not enabled for this job.\"]}");

        String fetchedCheckpointsJson = kvStore.get("/jobs/" + jobID + "/checkpoints");
        assertThat(fetchedCheckpointsJson)
                .isEqualTo("{\"errors\":[\"Checkpointing has not been enabled.\"]}");

        String fetchedPlanJson = kvStore.get("/jobs/" + jobID + "/plan");
        assertThat(fetchedPlanJson)
                .isEqualTo(
                        "{\"plan\":{\"jid\":\"simpleJobID\",\"name\":\"Example Job\",\"type\":\"BATCH\",\"nodes\":[{\"id\":\"node1\",\"operator\":\"Source\",\"description\":\"Source Description\"}]}}");
    }

    @Test
    void testProcessAndFetchMultipleArchiveFiles() throws Exception {
        String jobIDPrefix = "multiFileJobID";
        int numArchives = 1000; // Define the number of archives you want to create and test
        List<java.nio.file.Path> jobArchives = createMultipleMockArchives(jobIDPrefix, numArchives);

        // Process each archive
        for (java.nio.file.Path jobArchive : jobArchives) {
            String archiveJobID = jobArchive.getFileName().toString().replace(".json", "");
            archiveFetcher.processArchive(archiveJobID, new Path(jobArchive.toUri()));
        }

        // Verify the data in RocksDB for each job ID
        for (int i = 1; i <= numArchives; i++) {
            String currentJobID =
                    jobIDPrefix
                            + String.format(
                                    "%08d", i); // Generates IDs like multiFileJobID00000001,
            // multiFileJobID00000002, etc.
            List<String> fetchedJsons = kvStore.getAllByPrefix("/jobs/" + currentJobID);

            // Verify that the fetched JSONs contain the expected 5 entries
            assertThat(fetchedJsons).hasSize(5);
            assertThat(fetchedJsons)
                    .contains(
                            "{\"jvm\":{\"version\":\"Java HotSpot(TM) 64-Bit Server VM\",\"arch\":\"amd64\",\"options\":[\"-Xmx1024m\",\"-Xms1024m\"]}}",
                            "{\"jid\":\""
                                    + currentJobID
                                    + "\",\"name\":\"Example Job\",\"execution-config\":{\"execution-mode\":\"PIPELINED\",\"restart-strategy\":\"Restart with fixed delay\",\"job-parallelism\":1}}",
                            "{\"errors\":[\"Checkpointing is not enabled for this job.\"]}",
                            "{\"errors\":[\"Checkpointing has not been enabled.\"]}",
                            "{\"plan\":{\"jid\":\""
                                    + currentJobID
                                    + "\",\"name\":\"Example Job\",\"type\":\"BATCH\",\"nodes\":[{\"id\":\"node1\",\"operator\":\"Source\",\"description\":\"Source Description\"}]}}");

            // Verify the special overview entry separately
            String overviewJson = kvStore.get("/jobs/overview/" + currentJobID);
            assertThat(overviewJson)
                    .isEqualTo(
                            "{\"jobs\":[{\"jid\":\""
                                    + currentJobID
                                    + "\",\"name\":\"Flink Streaming Job\",\"state\":\"FINISHED\",\"start-time\":1723270676500,\"end-time\":1723270676621,\"duration\":121,\"last-modification\":1723270676621,\"tasks\":{\"total\":3,\"created\":0,\"scheduled\":0,\"deploying\":0,\"running\":0,\"finished\":3,\"canceling\":0,\"canceled\":0,\"failed\":0,\"reconciling\":0,\"initializing\":0}}]}");
        }
    }

    @Test
    void testCompareRocksDBVsFileSystemInodesAndSize() throws Exception {
        String jobIDPrefix = "multiFileJobID";
        int numArchives = 400; // Define the number of archives you want to create and test
        List<java.nio.file.Path> jobArchives = createMultipleMockArchives(jobIDPrefix, numArchives);

        // Process each archive and save to RocksDB
        for (java.nio.file.Path jobArchive : jobArchives) {
            String archiveJobID = jobArchive.getFileName().toString().replace(".json", "");
            archiveFetcher.processArchive(archiveJobID, new Path(jobArchive.toUri()));
        }

        // Count the number of inodes (files and directories) used by RocksDB
        long totalRocksDBInodes =
                Files.walk(tmpDir.resolve("rocksdb")).count(); // Counts both files and directories

        // Measure the size of the RocksDB database directory
        long totalRocksDBSize =
                Files.walk(tmpDir.resolve("rocksdb"))
                        .filter(Files::isRegularFile)
                        .mapToLong(path -> path.toFile().length())
                        .sum();

        // Save the same data to a static file system with proper directory structure
        java.nio.file.Path fileSystemDir = tmpDir.resolve("filesystem");
        Files.createDirectories(fileSystemDir);

        for (java.nio.file.Path jobArchive : jobArchives) {
            // Read the content of the archive
            String archiveContent = new String(Files.readAllBytes(jobArchive));
            ObjectMapper mapper = new ObjectMapper();
            JsonNode archiveNode = mapper.readTree(archiveContent).get("archive");

            // Extract the jobID from the file name
            String jobID = jobArchive.getFileName().toString().replace(".json", "");

            // Iterate through each entry in the archive and store in the appropriate directory
            for (JsonNode archiveEntry : archiveNode) {
                String path = archiveEntry.get("path").asText();
                String json = archiveEntry.get("json").asText();

                // Create the full directory path
                java.nio.file.Path directoryPath =
                        fileSystemDir.resolve(path.substring(1)); // Remove leading slash
                Files.createDirectories(directoryPath); // Ensure the parent directories exist

                // Create a file named jobID.json under the directory
                java.nio.file.Path targetFilePath = directoryPath.resolve(jobID + ".json");

                // Write the JSON content to the file
                Files.write(targetFilePath, json.getBytes());
            }
        }

        // Count the number of inodes (files and directories) used by the file system
        long totalFileSystemInodes =
                Files.walk(fileSystemDir).count(); // Counts both files and directories

        // Measure the size of the file system directory
        long totalFileSystemSize =
                Files.walk(fileSystemDir)
                        .filter(Files::isRegularFile)
                        .mapToLong(path -> path.toFile().length())
                        .sum();

        // Output the inode counts and sizes
        System.out.println("Total RocksDB inodes: " + totalRocksDBInodes);
        System.out.println("Total File System inodes: " + totalFileSystemInodes);
        System.out.println("Total RocksDB size: " + totalRocksDBSize + " bytes");
        System.out.println("Total File System size: " + totalFileSystemSize + " bytes");

        // Assert that the RocksDB inode count is less than or equal to the file system inode count
        assertThat(totalRocksDBInodes).isLessThanOrEqualTo(totalFileSystemInodes);
        // assertThat(totalRocksDBSize).isLessThanOrEqualTo(totalFileSystemSize);
    }

    @Test
    void testUpdateJobOverview() throws Exception {
        // Use fixed job IDs
        String jobID1 = "00000000000000000000000000000001";
        String jobID2 = "00000000000000000000000000000002";
        String jobID3 = "00000000000000000000000000000003";

        // Create and process three mock archives
        java.nio.file.Path jobArchive1 = createOneMockArchive(jobID1);
        java.nio.file.Path jobArchive2 = createOneMockArchive(jobID2);
        java.nio.file.Path jobArchive3 = createOneMockArchive(jobID3);

        archiveFetcher.processArchive(jobID1, new Path(jobArchive1.toUri()));
        archiveFetcher.processArchive(jobID2, new Path(jobArchive2.toUri()));
        archiveFetcher.processArchive(jobID3, new Path(jobArchive3.toUri()));

        // Invoke the updateJobOverview method that aggregates individual job overviews.
        updateJobOverview(kvStore);

        // The expected combined overview JSON. Note that updateJobOverview adds
        String expectedCombinedOverview =
                "{\"jobs\":["
                        + "{\"jid\":\""
                        + jobID1
                        + "\",\"name\":\"Flink Streaming Job\",\"start-time\":1723270676500,"
                        + "\"end-time\":1723270676621,\"duration\":121,\"state\":\"FINISHED\",\"last-modification\":1723270676621,"
                        + "\"tasks\":{\"running\":0,\"canceling\":0,\"canceled\":0,\"total\":3,\"created\":0,\"scheduled\":0,"
                        + "\"deploying\":0,\"reconciling\":0,\"finished\":3,\"initializing\":0,\"failed\":0},\"pending-operators\":0},"
                        + "{\"jid\":\""
                        + jobID2
                        + "\",\"name\":\"Flink Streaming Job\",\"start-time\":1723270676500,"
                        + "\"end-time\":1723270676621,\"duration\":121,\"state\":\"FINISHED\",\"last-modification\":1723270676621,"
                        + "\"tasks\":{\"running\":0,\"canceling\":0,\"canceled\":0,\"total\":3,\"created\":0,\"scheduled\":0,"
                        + "\"deploying\":0,\"reconciling\":0,\"finished\":3,\"initializing\":0,\"failed\":0},\"pending-operators\":0},"
                        + "{\"jid\":\""
                        + jobID3
                        + "\",\"name\":\"Flink Streaming Job\",\"start-time\":1723270676500,"
                        + "\"end-time\":1723270676621,\"duration\":121,\"state\":\"FINISHED\",\"last-modification\":1723270676621,"
                        + "\"tasks\":{\"running\":0,\"canceling\":0,\"canceled\":0,\"total\":3,\"created\":0,\"scheduled\":0,"
                        + "\"deploying\":0,\"reconciling\":0,\"finished\":3,\"initializing\":0,\"failed\":0},\"pending-operators\":0}"
                        + "]}";

        // Retrieve the combined overview from the KV store.
        String combinedOverviewJson = kvStore.get("/jobs/combined-overview");

        // Use Jackson's ObjectMapper to compare JSON structures instead of raw strings.
        ObjectMapper mapper = new ObjectMapper();
        JsonNode expectedNode = mapper.readTree(expectedCombinedOverview);
        JsonNode actualNode = mapper.readTree(combinedOverviewJson);

        // Assert that both JSON structures are equivalent.
        assertThat(actualNode).isEqualTo(expectedNode);

        System.out.println("combinedOverviewJson: " + combinedOverviewJson);
    }

    private java.nio.file.Path createOneMockArchive(String jobID) throws IOException {
        String json =
                "{\n"
                        + "  \"archive\": [\n"
                        + "    {\n"
                        + "      \"path\": \"/jobs/"
                        + jobID
                        + "/jobmanager/environment\",\n"
                        + "      \"json\": \"{\\\"jvm\\\":{\\\"version\\\":\\\"Java HotSpot(TM) 64-Bit Server VM\\\",\\\"arch\\\":\\\"amd64\\\",\\\"options\\\":[\\\"-Xmx1024m\\\",\\\"-Xms1024m\\\"]}}\"\n"
                        + "    },\n"
                        + "    {\n"
                        + "      \"path\": \"/jobs/overview\",\n"
                        + "      \"json\": \"{\\\"jobs\\\":[{\\\"jid\\\":\\\""
                        + jobID
                        + "\\\",\\\"name\\\":\\\"Flink Streaming Job\\\",\\\"state\\\":\\\"FINISHED\\\",\\\"start-time\\\":1723270676500,\\\"end-time\\\":1723270676621,\\\"duration\\\":121,\\\"last-modification\\\":1723270676621,\\\"tasks\\\":{\\\"total\\\":3,\\\"created\\\":0,\\\"scheduled\\\":0,\\\"deploying\\\":0,\\\"running\\\":0,\\\"finished\\\":3,\\\"canceling\\\":0,\\\"canceled\\\":0,\\\"failed\\\":0,\\\"reconciling\\\":0,\\\"initializing\\\":0}}]}\"\n"
                        + "    },\n"
                        + "    {\n"
                        + "      \"path\": \"/jobs/"
                        + jobID
                        + "/config\",\n"
                        + "      \"json\": \"{\\\"jid\\\":\\\""
                        + jobID
                        + "\\\",\\\"name\\\":\\\"Example Job\\\",\\\"execution-config\\\":{\\\"execution-mode\\\":\\\"PIPELINED\\\",\\\"restart-strategy\\\":\\\"Restart with fixed delay\\\",\\\"job-parallelism\\\":1}}\"\n"
                        + "    },\n"
                        + "    {\n"
                        + "      \"path\": \"/jobs/"
                        + jobID
                        + "/checkpoints/config\",\n"
                        + "      \"json\": \"{\\\"errors\\\":[\\\"Checkpointing is not enabled for this job.\\\"]}\"\n"
                        + "    },\n"
                        + "    {\n"
                        + "      \"path\": \"/jobs/"
                        + jobID
                        + "/checkpoints\",\n"
                        + "      \"json\": \"{\\\"errors\\\":[\\\"Checkpointing has not been enabled.\\\"]}\"\n"
                        + "    },\n"
                        + "    {\n"
                        + "      \"path\": \"/jobs/"
                        + jobID
                        + "/plan\",\n"
                        + "      \"json\": \"{\\\"plan\\\":{\\\"jid\\\":\\\""
                        + jobID
                        + "\\\",\\\"name\\\":\\\"Example Job\\\",\\\"type\\\":\\\"BATCH\\\",\\\"nodes\\\":[{\\\"id\\\":\\\"node1\\\",\\\"operator\\\":\\\"Source\\\",\\\"description\\\":\\\"Source Description\\\"}]}}\"\n"
                        + "    }\n"
                        + "  ]\n"
                        + "}";
        java.nio.file.Path archiveFile = Files.createFile(tmpDir.resolve(jobID + ".json"));
        Files.write(archiveFile, json.getBytes());
        return archiveFile;
    }

    private List<java.nio.file.Path> createMultipleMockArchives(String jobIDPrefix, int numArchives)
            throws IOException {
        List<java.nio.file.Path> archivePaths = new ArrayList<>();

        for (int i = 1; i <= numArchives; i++) {
            // Generate a new jobID for each archive with leading zeros
            String modifiedJobID = jobIDPrefix + String.format("%08d", i);

            // Create a mock archive using the existing createOneMockArchive method
            java.nio.file.Path archivePath = createOneMockArchive(modifiedJobID);

            // Add the created archive path to the list
            archivePaths.add(archivePath);
        }

        return archivePaths;
    }
}
