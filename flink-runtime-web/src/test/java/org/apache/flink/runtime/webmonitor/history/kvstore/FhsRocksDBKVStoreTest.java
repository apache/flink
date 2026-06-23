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

package org.apache.flink.runtime.webmonitor.history.kvstore;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.RocksDB;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for {@link HistoryServerRocksDBKVStore}. */
public class FhsRocksDBKVStoreTest {

    private static final String DB_PATH = "test-db";

    private KVStore<String, String> kvStore;

    @Before
    public void setUp() throws Exception {
        RocksDB.loadLibrary();
        kvStore = new HistoryServerRocksDBKVStore(new File(DB_PATH));
    }

    @After
    public void tearDown() throws Exception {
        kvStore.close();
        deleteDirectory(new File(DB_PATH));
    }

    @Test
    public void testPutAndGet() throws Exception {
        kvStore.put("key1", "value1");
        String value = kvStore.get("key1");
        assertEquals("value1", value);
    }

    @Test
    public void testUpdate() throws Exception {
        kvStore.put("key1", "value1");
        kvStore.put("key1", "value2");
        String value = kvStore.get("key1");
        assertEquals("value2", value);
    }

    @Test
    public void testDelete() throws Exception {
        kvStore.put("key2", "value2");
        kvStore.delete("key2");
        String value = kvStore.get("key2");
        assertNull(value);
    }

    @Test
    public void testDeleteNonExistentKey() throws Exception {
        kvStore.delete("nonExistentKey");
        String value = kvStore.get("nonExistentKey");
        assertNull(value);
    }

    @Test
    public void testWriteAll() throws Exception {
        Map<String, String> entries = new HashMap<>();
        entries.put("key3", "value3");
        entries.put("key4", "value4");

        kvStore.writeAll(entries);

        assertEquals("value3", kvStore.get("key3"));
        assertEquals("value4", kvStore.get("key4"));
    }

    @Test
    public void testEmptyKeyAndValue() throws Exception {
        // Test with empty key and value
        try {
            kvStore.put("", "");
            fail("Expected an exception when putting empty key and value");
        } catch (IllegalArgumentException e) {
            // Expected behavior, handle specific exceptions if necessary
        }
    }

    @Test
    public void testSpecialCharacters() throws Exception {
        String specialKey = "!@#$%^&*()_+{}:\"<>?";
        String specialValue = "~`-=[]\\;',./";
        kvStore.put(specialKey, specialValue);
        String value = kvStore.get(specialKey);
        assertEquals(specialValue, value);
    }

    @Test
    public void testLargeData() throws Exception {
        String largeKey = "largeKey";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1000000; i++) {
            sb.append("largeValue");
        }
        String largeValue = sb.toString();
        kvStore.put(largeKey, largeValue);
        String value = kvStore.get(largeKey);
        assertEquals(largeValue, value);
    }

    @Test
    public void testVeryLargeVolume() throws Exception {
        Map<String, String> entries = new HashMap<>();
        for (int i = 0; i < 1000000; i++) {
            entries.put("key" + i, "value" + i);
        }
        kvStore.writeAll(entries);

        for (int i = 0; i < 1000000; i++) {
            assertEquals("value" + i, kvStore.get("key" + i));
        }
    }

    @Test
    public void testOverwriteValueForSameKey() throws Exception {
        String key = "overwriteKey";
        kvStore.put(key, "initialValue");
        assertEquals("initialValue", kvStore.get(key));

        kvStore.put(key, "newValue");
        assertEquals("newValue", kvStore.get(key));
    }

    @Test
    public void testNonExistentKey() throws Exception {
        String value = kvStore.get("nonExistentKey");
        assertNull(value);
    }

    @Test
    public void testGetAllByPrefix() throws Exception {
        String jobId = "testJob";
        kvStore.put("/jobs/" + jobId + "/jobmanager/environment", "{\"info\":\"env\"}");
        kvStore.put("/jobs/" + jobId + "/config", "{\"info\":\"config\"}");
        kvStore.put("/jobs/" + jobId + "/overview", "{\"info\":\"overview\"}");
        kvStore.put("/jobs/otherJob/config", "{\"info\":\"otherJobConfig\"}");
        // Add a key that includes the jobId but does not start with the prefix
        kvStore.put("/someprefix/jobs/" + jobId + "/irrelevant", "{\"info\":\"irrelevant\"}");

        List<String> jobJsons = kvStore.getAllByPrefix("/jobs/" + jobId);
        assertEquals(3, jobJsons.size());
        assertTrue(jobJsons.contains("{\"info\":\"env\"}"));
        assertTrue(jobJsons.contains("{\"info\":\"config\"}"));
        assertTrue(jobJsons.contains("{\"info\":\"overview\"}"));
    }

    private static void deleteDirectory(File directory) throws IOException {
        if (directory.exists()) {
            Files.walk(directory.toPath()).map(java.nio.file.Path::toFile).forEach(File::delete);
        }
    }
}
