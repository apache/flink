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

package org.apache.flink.python.util;

import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.PythonOptions;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.flink.python.PythonOptions.PYTHON_ARCHIVES_DISTRIBUTED_CACHE_INFO;
import static org.apache.flink.python.PythonOptions.PYTHON_CLIENT_EXECUTABLE;
import static org.apache.flink.python.PythonOptions.PYTHON_EXECUTABLE;
import static org.apache.flink.python.PythonOptions.PYTHON_FILES_DISTRIBUTED_CACHE_INFO;
import static org.apache.flink.python.PythonOptions.PYTHON_REQUIREMENTS;
import static org.apache.flink.python.PythonOptions.PYTHON_REQUIREMENTS_FILE_DISTRIBUTED_CACHE_INFO;
import static org.apache.flink.python.util.PythonDependencyUtils.CACHE;
import static org.apache.flink.python.util.PythonDependencyUtils.FILE;
import static org.apache.flink.python.util.PythonDependencyUtils.configurePythonDependencies;
import static org.apache.flink.python.util.PythonDependencyUtils.merge;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for PythonDependencyUtils. */
class PythonDependencyUtilsTest {

    private List<Tuple2<String, DistributedCache.DistributedCacheEntry>> cachedFiles;

    @BeforeEach
    void setUp() {
        cachedFiles = new ArrayList<>();
    }

    @Test
    void testPythonFiles() {
        Configuration config = new Configuration();
        config.set(
                PythonOptions.PYTHON_FILES,
                "hdfs:///tmp_dir/test_file1.py,tmp_dir/test_file2.py,tmp_dir/test_dir,hdfs:///tmp_dir/test_file1.py");
        Configuration actual = configurePythonDependencies(cachedFiles, config);

        Map<String, String> expectedCachedFiles = new HashMap<>();
        expectedCachedFiles.put(
                "python_file_83bbdaee494ad7d9b334c02ec71dc86a0868f7f8e49d1249a37c517dc6ee15a7",
                "hdfs:///tmp_dir/test_file1.py");
        expectedCachedFiles.put(
                "python_file_e57a895cb1256500098be0874128680cd9f56000d48fcd393c48d6371bd2d947",
                "tmp_dir/test_file2.py");
        expectedCachedFiles.put(
                "python_file_e56bc55ff643576457b3d012b2bba888727c71cf05a958930f2263398c4e9798",
                "tmp_dir/test_dir");
        verifyCachedFiles(expectedCachedFiles);

        Configuration expectedConfiguration = new Configuration();
        expectedConfiguration.set(PYTHON_FILES_DISTRIBUTED_CACHE_INFO, new HashMap<>());
        expectedConfiguration
                .get(PYTHON_FILES_DISTRIBUTED_CACHE_INFO)
                .put(
                        "python_file_83bbdaee494ad7d9b334c02ec71dc86a0868f7f8e49d1249a37c517dc6ee15a7",
                        "test_file1.py");
        expectedConfiguration
                .get(PYTHON_FILES_DISTRIBUTED_CACHE_INFO)
                .put(
                        "python_file_e57a895cb1256500098be0874128680cd9f56000d48fcd393c48d6371bd2d947",
                        "test_file2.py");
        expectedConfiguration
                .get(PYTHON_FILES_DISTRIBUTED_CACHE_INFO)
                .put(
                        "python_file_e56bc55ff643576457b3d012b2bba888727c71cf05a958930f2263398c4e9798",
                        "test_dir");
        verifyConfiguration(expectedConfiguration, actual);
    }

    @Test
    void testPythonRequirements() {
        Configuration config = new Configuration();
        config.set(PYTHON_REQUIREMENTS, "tmp_dir/requirements.txt");
        Configuration actual = configurePythonDependencies(cachedFiles, config);

        Map<String, String> expectedCachedFiles = new HashMap<>();
        expectedCachedFiles.put(
                "python_requirements_file_69390ca43c69ada3819226fcfbb5b6d27e111132a9427e7f201edd82e9d65ff6",
                "tmp_dir/requirements.txt");
        verifyCachedFiles(expectedCachedFiles);

        Configuration expectedConfiguration = new Configuration();
        expectedConfiguration.set(PYTHON_REQUIREMENTS_FILE_DISTRIBUTED_CACHE_INFO, new HashMap<>());
        expectedConfiguration
                .get(PYTHON_REQUIREMENTS_FILE_DISTRIBUTED_CACHE_INFO)
                .put(
                        FILE,
                        "python_requirements_file_69390ca43c69ada3819226fcfbb5b6d27e111132a9427e7f201edd82e9d65ff6");
        verifyConfiguration(expectedConfiguration, actual);

        config.set(PYTHON_REQUIREMENTS, "tmp_dir/requirements2.txt#tmp_dir/cache");
        actual = configurePythonDependencies(cachedFiles, config);

        expectedCachedFiles = new HashMap<>();
        expectedCachedFiles.put(
                "python_requirements_file_56fd0c530faaa7129dca8d314cf69cbfc7c1c5c952f5176a003253e2f418873e",
                "tmp_dir/requirements2.txt");
        expectedCachedFiles.put(
                "python_requirements_cache_2f563dd6731c2c7c5e1ef1ef8279f61e907dc3bfc698adb71b109e43ed93e143",
                "tmp_dir/cache");
        verifyCachedFiles(expectedCachedFiles);

        expectedConfiguration = new Configuration();
        expectedConfiguration.set(PYTHON_REQUIREMENTS_FILE_DISTRIBUTED_CACHE_INFO, new HashMap<>());
        expectedConfiguration
                .get(PYTHON_REQUIREMENTS_FILE_DISTRIBUTED_CACHE_INFO)
                .put(
                        FILE,
                        "python_requirements_file_56fd0c530faaa7129dca8d314cf69cbfc7c1c5c952f5176a003253e2f418873e");
        expectedConfiguration
                .get(PYTHON_REQUIREMENTS_FILE_DISTRIBUTED_CACHE_INFO)
                .put(
                        CACHE,
                        "python_requirements_cache_2f563dd6731c2c7c5e1ef1ef8279f61e907dc3bfc698adb71b109e43ed93e143");
        verifyConfiguration(expectedConfiguration, actual);
    }

    @Test
    void testPythonArchives() {
        Configuration config = new Configuration();
        config.set(
                PythonOptions.PYTHON_ARCHIVES,
                "hdfs:///tmp_dir/file1.zip,"
                        + "hdfs:///tmp_dir/file1.zip,"
                        + "tmp_dir/py37.zip,"
                        + "tmp_dir/py37.zip#venv,"
                        + "tmp_dir/py37.zip#venv2,tmp_dir/py37.zip#venv");
        Configuration actual = configurePythonDependencies(cachedFiles, config);

        Map<String, String> expectedCachedFiles = new HashMap<>();
        expectedCachedFiles.put(
                "python_archive_4cc74e4003de886434723f351771df2a84f72531c52085acc0915e19d70df2ba",
                "hdfs:///tmp_dir/file1.zip");
        expectedCachedFiles.put(
                "python_archive_f8a1c874251230f21094880d9dd878ffb5714454b69184d8ad268a6563269f0c",
                "tmp_dir/py37.zip");
        expectedCachedFiles.put(
                "python_archive_5f3fca2a4165c7d9c94b00bfab956c15f14c41e9e03f6037c83eb61157fce09c",
                "tmp_dir/py37.zip");
        expectedCachedFiles.put(
                "python_archive_c7d970ce1c5794367974ce8ef536c2343bed8fcfe7c2422c51548e58007eee6a",
                "tmp_dir/py37.zip");
        verifyCachedFiles(expectedCachedFiles);

        Configuration expectedConfiguration = new Configuration();
        expectedConfiguration.set(PYTHON_ARCHIVES_DISTRIBUTED_CACHE_INFO, new HashMap<>());
        expectedConfiguration
                .get(PYTHON_ARCHIVES_DISTRIBUTED_CACHE_INFO)
                .put(
                        "python_archive_4cc74e4003de886434723f351771df2a84f72531c52085acc0915e19d70df2ba",
                        "file1.zip");
        expectedConfiguration
                .get(PYTHON_ARCHIVES_DISTRIBUTED_CACHE_INFO)
                .put(
                        "python_archive_5f3fca2a4165c7d9c94b00bfab956c15f14c41e9e03f6037c83eb61157fce09c",
                        "py37.zip");
        expectedConfiguration
                .get(PYTHON_ARCHIVES_DISTRIBUTED_CACHE_INFO)
                .put(
                        "python_archive_f8a1c874251230f21094880d9dd878ffb5714454b69184d8ad268a6563269f0c",
                        "py37.zip#venv2");
        expectedConfiguration
                .get(PYTHON_ARCHIVES_DISTRIBUTED_CACHE_INFO)
                .put(
                        "python_archive_c7d970ce1c5794367974ce8ef536c2343bed8fcfe7c2422c51548e58007eee6a",
                        "py37.zip#venv");
        verifyConfiguration(expectedConfiguration, actual);
    }

    @Test
    void testPythonExecutables() {
        Configuration config = new Configuration();
        config.set(PYTHON_EXECUTABLE, "venv/bin/python3");
        config.set(PYTHON_CLIENT_EXECUTABLE, "python37");
        Configuration actual = configurePythonDependencies(cachedFiles, config);

        Configuration expectedConfiguration = new Configuration();
        expectedConfiguration.set(PYTHON_EXECUTABLE, "venv/bin/python3");
        expectedConfiguration.set(PYTHON_CLIENT_EXECUTABLE, "python37");
        verifyConfiguration(expectedConfiguration, actual);
    }

    @Test
    void testPythonDependencyConfigMerge() {
        Configuration config = new Configuration();
        config.set(
                PythonOptions.PYTHON_ARCHIVES,
                "hdfs:///tmp_dir/file1.zip,hdfs:///tmp_dir/file2.zip");
        config.set(
                PythonOptions.PYTHON_FILES, "hdfs:///tmp_dir/file3.zip,hdfs:///tmp_dir/file4.zip");

        Configuration config2 = new Configuration();
        config2.set(
                PythonOptions.PYTHON_ARCHIVES,
                "hdfs:///tmp_dir/file5.zip,hdfs:///tmp_dir/file6.zip");
        config2.set(
                PythonOptions.PYTHON_FILES, "hdfs:///tmp_dir/file7.zip,hdfs:///tmp_dir/file8.zip");

        Configuration expectedConfiguration = new Configuration();
        expectedConfiguration.set(
                PythonOptions.PYTHON_ARCHIVES,
                "hdfs:///tmp_dir/file5.zip,hdfs:///tmp_dir/file6.zip,hdfs:///tmp_dir/file1.zip,hdfs:///tmp_dir/file2.zip");
        expectedConfiguration.set(
                PythonOptions.PYTHON_FILES,
                "hdfs:///tmp_dir/file7.zip,hdfs:///tmp_dir/file8.zip,hdfs:///tmp_dir/file3.zip,hdfs:///tmp_dir/file4.zip");
        merge(config, config2);
        verifyConfiguration(expectedConfiguration, config);
    }

    @Test
    void testPythonPath() {
        String pyPath =
                "venv/bin/python3/lib64/python3.7/site-packages/:venv/bin/python3/lib/python3.7/site-packages/";
        Configuration config = new Configuration();
        config.set(PythonOptions.PYTHON_PATH, pyPath);
        Configuration actual = configurePythonDependencies(cachedFiles, config);
        Configuration expectedConfiguration = new Configuration();
        expectedConfiguration.set(PythonOptions.PYTHON_PATH, pyPath);
        verifyConfiguration(expectedConfiguration, actual);
    }

    private void verifyCachedFiles(Map<String, String> expected) {
        Map<String, String> actual =
                cachedFiles.stream().collect(Collectors.toMap(t -> t.f0, t -> t.f1.filePath));

        assertThat(actual).isEqualTo(expected);
    }

    private void verifyConfiguration(Configuration expected, Configuration actual) {
        Properties actualProperties = new Properties();
        actual.addAllToProperties(actualProperties);
        Properties expectedProperties = new Properties();
        expected.addAllToProperties(expectedProperties);
        assertThat(actualProperties).isEqualTo(expectedProperties);
    }
}
