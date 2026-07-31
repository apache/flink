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

import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Basic tests for {@link ArchiveStorage} implementations.
 *
 * <p>New {@link ArchiveStorage} implementations only need to register an instance in {@link
 * #storageFactories()} to be covered by these tests.
 */
@ExtendWith(ParameterizedTestExtension.class)
class ArchiveStorageTest {

    @TempDir private File tempDir;

    @Parameter public ArchiveStorageFactory<Object> storageFactory;

    private ArchiveStorage<Object> storage;

    @Parameters(name = "storageFactory={0}")
    private static Collection<ArchiveStorageFactory<?>> storageFactories() {
        ArchiveStorageFactory<File> fileArchiveStorageFactory = FileArchiveStorage::new;
        ArchiveStorageFactory<String> rocksDBStorageFactory = RocksDBArchiveStorage::new;
        return List.of(fileArchiveStorageFactory, rocksDBStorageFactory);
    }

    /** Creates an {@link ArchiveStorage} instance under the given temporary directory. */
    @FunctionalInterface
    interface ArchiveStorageFactory<T> {
        ArchiveStorage<T> create(File tempDir) throws Exception;
    }

    @BeforeEach
    void setUp() throws Exception {
        storage = storageFactory.create(tempDir);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (storage != null) {
            storage.close();
        }
    }

    // ----------------------------- exists / put / get / delete ----------------------------

    /**
     * Covers the whole lifecycle of a single key across {@code exists}, {@code put}, {@code get}
     * and {@code delete}.
     */
    @TestTemplate
    void testBasicLifecycle() throws Exception {
        String key = "overviews/abc123.json";
        String content = "{\"test\":\"data\"}";

        // exists: missing key
        assertThat(storage.exists(key)).isFalse();

        // put + exists + get returns the same content
        storage.putArchiveContent(key, content);
        assertThat(storage.exists(key)).isTrue();
        assertThat(storage.readArchiveContent(storage.getEntry(key))).isEqualTo(content);

        // delete + exists + get returns null
        storage.delete(key);
        assertThat(storage.exists(key)).isFalse();
        assertThat(storage.getEntry(key)).isNull();
    }

    @TestTemplate
    void testOverwrite() throws Exception {
        String key = "overviews/abc123.json";
        String content = "{\"test\":\"data\"}";

        // put + exists + get returns the same content
        storage.putArchiveContent(key, content);
        assertThat(storage.exists(key)).isTrue();
        assertThat(storage.readArchiveContent(storage.getEntry(key))).isEqualTo(content);

        // put again: the latest content overwrites the previous one
        String overwriteContent = "{\"test\":\"overwrite_data\"}";
        storage.putArchiveContent(key, overwriteContent);
        assertThat(storage.exists(key)).isTrue();
        assertThat(storage.readArchiveContent(storage.getEntry(key))).isEqualTo(overwriteContent);
    }

    // ----------------------------- deleteEntriesByPrefix ----------------------------

    @TestTemplate
    void testDeleteEntriesByPrefix() throws Exception {
        String keyUnderPrefix1 = "jobs/abc123/config.json";
        String keyUnderPrefix2 = "jobs/abc123/vertices.json";
        String keyOutsidePrefix = "jobs/def456/config.json";
        storage.putArchiveContent(keyUnderPrefix1, "{\"config\":\"under_prefix_data\"}");
        storage.putArchiveContent(keyUnderPrefix2, "{\"vertices\":\"under_prefix_data\"}");
        storage.putArchiveContent(keyOutsidePrefix, "{\"config\":\"outside_prefix_data\"}");

        storage.deleteEntriesByPrefix("jobs/abc123/");

        assertThat(storage.exists(keyUnderPrefix1)).isFalse();
        assertThat(storage.exists(keyUnderPrefix2)).isFalse();
        assertThat(storage.exists(keyOutsidePrefix)).isTrue();
        assertThat(storage.readArchiveContent(storage.getEntry(keyOutsidePrefix)))
                .isEqualTo("{\"config\":\"outside_prefix_data\"}");
    }

    @TestTemplate
    void testDeleteEntriesByPrefixDoesNotTouchSiblingKey() throws Exception {
        String keyUnderPrefix = "jobs/abc123/config.json";
        String siblingKey = "jobs/abc123.json";
        storage.putArchiveContent(keyUnderPrefix, "{\"config\":\"under_prefix_data\"}");
        storage.putArchiveContent(siblingKey, "{\"sibling\":\"data\"}");

        storage.deleteEntriesByPrefix("jobs/abc123/");

        assertThat(storage.exists(keyUnderPrefix)).isFalse();
        assertThat(storage.exists(siblingKey)).isTrue();
        assertThat(storage.readArchiveContent(storage.getEntry(siblingKey)))
                .isEqualTo("{\"sibling\":\"data\"}");
    }

    // ----------------------------- getEntriesByPrefix ----------------------------

    @TestTemplate
    void testGetEntriesByPrefix() throws Exception {
        Map<String, String> entriesUnderPrefix = new HashMap<>();
        entriesUnderPrefix.put("overviews/job1.json", "{\"job\":\"job1\"}");
        entriesUnderPrefix.put("overviews/job2.json", "{\"job\":\"job2\"}");
        entriesUnderPrefix.put("overviews/job3.json", "{\"job\":\"job3\"}");
        String keyOutsidePrefix = "jobs/job1/config.json";

        for (Map.Entry<String, String> e : entriesUnderPrefix.entrySet()) {
            storage.putArchiveContent(e.getKey(), e.getValue());
        }
        storage.putArchiveContent(keyOutsidePrefix, "{\"config\":\"data\"}");

        List<Object> result = storage.getEntriesByPrefix("overviews/");
        List<String> contents = new ArrayList<>();
        for (Object entry : result) {
            contents.add(storage.readArchiveContent(entry));
        }
        assertThat(contents).containsExactlyInAnyOrderElementsOf(entriesUnderPrefix.values());
    }

    @TestTemplate
    void testGetEntriesByPrefixDoesNotIncludeSiblingKey() throws Exception {
        Map<String, String> entriesUnderPrefix = new HashMap<>();
        entriesUnderPrefix.put("overviews/job1.json", "{\"job\":\"job1\"}");
        entriesUnderPrefix.put("overviews/job2.json", "{\"job\":\"job2\"}");
        String siblingKey = "overviews-legacy.json";

        for (Map.Entry<String, String> e : entriesUnderPrefix.entrySet()) {
            storage.putArchiveContent(e.getKey(), e.getValue());
        }
        storage.putArchiveContent(siblingKey, "{\"sibling\":\"data\"}");

        List<Object> result = storage.getEntriesByPrefix("overviews/");
        List<String> contents = new ArrayList<>();
        for (Object entry : result) {
            contents.add(storage.readArchiveContent(entry));
        }
        assertThat(contents).containsExactlyInAnyOrderElementsOf(entriesUnderPrefix.values());
    }
}
