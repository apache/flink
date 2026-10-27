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

package org.apache.flink.state.catalog;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link SnapshotDiscovery}. */
class SnapshotDiscoveryTest {

    // Every metadata file created by createMetadataFile() gets this exact modification time, so
    // the creationTs segment in derived database names is deterministic across all tests.
    private static final Instant FIXED_TS = Instant.parse("2024-03-15T10:30:45Z");
    private static final String TS = "2024-03-15T10:30:45Z";

    private static final String MISSING_DIR = "/nonexistent-snapshot-discovery-test-path";

    @TempDir Path tempDir;

    private final List<SnapshotDiscovery> started = new ArrayList<>();

    private SnapshotDiscovery discovery;

    @BeforeEach
    void setUp() {
        discovery = start(Collections.singletonMap("app", tempDir.toString()), true);
    }

    @AfterEach
    void tearDown() {
        started.forEach(SnapshotDiscovery::stop);
    }

    // -------------------------------------------------------------------------
    // Construction-time validation
    // -------------------------------------------------------------------------

    @Test
    void testConstructionValidation() {
        assertThatThrownBy(() -> new SnapshotDiscovery(Collections.emptyMap(), 2, true))
                .as("no directory configured")
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> new SnapshotDiscovery(dirs("/state/app", "/state/app"), 2, true))
                .as("same directory under two labels")
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> new SnapshotDiscovery(dirs("/state", "/state/app"), 2, true))
                .as("one directory nested inside the other")
                .isInstanceOf(IllegalArgumentException.class);
    }

    // -------------------------------------------------------------------------
    // find()
    // -------------------------------------------------------------------------

    @Test
    void testFindRejectsInvalidDatabaseNames() {
        assertThat(discovery.find(null)).isEmpty();
        assertThat(discovery.find("")).isEmpty();
        assertThat(discovery.find("   ")).isEmpty();
        assertThat(discovery.find("/savepoint-abc")).isEmpty();
        assertThat(discovery.find("unknown/" + TS + "/savepoint-abc")).isEmpty();

        // With db-name.include-ts enabled (the default), a name with no '/' has no room for the
        // mandatory creationTs segment, so it can never match.
        assertThat(discovery.find("app")).isEmpty();
        assertThat(discovery.find("savepoint-abc")).isEmpty();
    }

    @Test
    void testFindResolvesRelativePathVerbatim() throws IOException {
        createMetadataFile(tempDir.resolve("savepoint-abc"));
        createMetadataFile(tempDir.resolve("jobId").resolve("chk-3"));
        createMetadataFile(tempDir.resolve("a").resolve("b").resolve("c"));

        assertThat(discovery.find("app/" + TS + "/savepoint-abc"))
                .hasValue(tempDir.resolve("savepoint-abc").toString());
        assertThat(discovery.find("app/" + TS + "/jobId/chk-3"))
                .hasValue(tempDir.resolve("jobId").resolve("chk-3").toString());
        assertThat(discovery.find("app/" + TS + "/a/b/c"))
                .hasValue(tempDir.resolve("a").resolve("b").resolve("c").toString());

        assertThat(discovery.find("app/" + TS + "/savepoint-nonexistent")).isEmpty();
    }

    @Test
    void testFindSnapshotWithTrailingSlash() throws IOException {
        createMetadataFile(tempDir.resolve("savepoint-abc"));

        // trailing slash after the creationTs → relativePath is empty, same as a ts-only path
        assertThat(discovery.find("app/" + TS + "/")).isEmpty();
    }

    @Test
    void testFindTsOnlyPathMatchesSnapshotDirectlyUnderConfiguredDir() throws IOException {
        createMetadataFile(tempDir);

        assertThat(discovery.find("app/" + TS)).hasValue(tempDir.toString());
    }

    @Test
    void testFindWithTsDisabled() throws IOException {
        SnapshotDiscovery noTs = start(Collections.singletonMap("app", tempDir.toString()), false);
        createMetadataFile(tempDir.resolve("savepoint-abc"));

        assertThat(noTs.find("app/savepoint-abc"))
                .hasValue(tempDir.resolve("savepoint-abc").toString());
        // With db-name.include-ts disabled, everything after the label is taken verbatim as the
        // relative path — no segment is skipped as a timestamp.
        assertThat(noTs.find("app/extra-segment/savepoint-abc")).isEmpty();
    }

    // -------------------------------------------------------------------------
    // list()
    // -------------------------------------------------------------------------

    @Test
    void testListReflectsFilesystemChangesWithoutCaching() throws IOException {
        assertThat(discovery.list()).isEmpty();

        createMetadataFile(tempDir.resolve("savepoint-new"));
        assertThat(discovery.list()).containsExactly("app/" + TS + "/savepoint-new");

        Files.delete(tempDir.resolve("savepoint-new").resolve("_metadata"));
        assertThat(discovery.list()).isEmpty();
    }

    @Test
    void testListMultipleSnapshots() throws IOException {
        createMetadataFile(tempDir.resolve("savepoint-a"));
        createMetadataFile(tempDir.resolve("savepoint-b"));
        createMetadataFile(tempDir.resolve("jobId").resolve("chk-1"));

        assertThat(discovery.list())
                .containsExactlyInAnyOrder(
                        "app/" + TS + "/savepoint-a",
                        "app/" + TS + "/savepoint-b",
                        "app/" + TS + "/jobId/chk-1");
    }

    @Test
    void testListNonMetadataFilesIgnored() throws IOException {
        Files.createDirectories(tempDir.resolve("savepoint-a"));
        Files.createFile(tempDir.resolve("savepoint-a").resolve("other.file"));

        assertThat(discovery.list()).isEmpty();
    }

    @Test
    void testListReturnsSnapshotsFromHealthyDirectoryWhenOtherFails() throws IOException {
        createMetadataFile(tempDir.resolve("savepoint-ok"));

        Map<String, String> labelToDir = new LinkedHashMap<>();
        labelToDir.put("good", tempDir.toString());
        labelToDir.put("bad", MISSING_DIR);

        assertThat(start(labelToDir, true).list()).containsExactly("good/" + TS + "/savepoint-ok");
    }

    @Test
    void testListThrowsWhenAllDirectoriesFail() {
        SnapshotDiscovery allBad = start(Collections.singletonMap("bad", MISSING_DIR), true);

        assertThatThrownBy(allBad::list)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("All configured directories failed")
                .cause()
                .isInstanceOf(IOException.class)
                .hasMessageContaining("All directory listings failed");
    }

    @Test
    void testListWithTsDisabled() throws IOException {
        SnapshotDiscovery noTs = start(Collections.singletonMap("app", tempDir.toString()), false);
        createMetadataFile(tempDir.resolve("savepoint-a"));
        createMetadataFile(tempDir.resolve("jobId").resolve("chk-1"));

        assertThat(noTs.list()).containsExactlyInAnyOrder("app/savepoint-a", "app/jobId/chk-1");
    }

    @Test
    void testDbNameCreationTsMatchesExpectedFormat() throws IOException {
        // Uses the real (unset) modification time to verify the formatter itself, rather than the
        // fixed FIXED_TS used elsewhere in this file.
        Files.createDirectories(tempDir.resolve("savepoint-live"));
        Files.createFile(tempDir.resolve("savepoint-live").resolve("_metadata"));

        assertThat(discovery.list().get(0))
                .matches("app/\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z/savepoint-live");
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private SnapshotDiscovery start(Map<String, String> labelToDir, boolean dbNameIncludeTs) {
        SnapshotDiscovery snapshotDiscovery = new SnapshotDiscovery(labelToDir, 2, dbNameIncludeTs);
        snapshotDiscovery.start();
        started.add(snapshotDiscovery);
        return snapshotDiscovery;
    }

    private static Map<String, String> dirs(String firstDir, String secondDir) {
        Map<String, String> labelToDir = new LinkedHashMap<>();
        labelToDir.put("a", firstDir);
        labelToDir.put("b", secondDir);
        return labelToDir;
    }

    private static void createMetadataFile(Path snapshotDir) throws IOException {
        Files.createDirectories(snapshotDir);
        Path file = Files.createFile(snapshotDir.resolve("_metadata"));
        Files.setLastModifiedTime(file, FileTime.from(FIXED_TS));
    }
}
