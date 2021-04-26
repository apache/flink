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

package org.apache.flink.table.planner.utils;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;

import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;

/** The base class for json plan testing. */
public abstract class JsonPlanTestBase {

    @Rule public ExpectedException exception = ExpectedException.none();

    @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

    protected TableEnvironmentInternal tableEnv;

    @Before
    public void setup() {
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        tableEnv = (TableEnvironmentInternal) TableEnvironment.create(settings);
    }

    protected void createTestCsvSourceTable(
            String tableName, List<String> data, String... fieldNameAndTypes) throws IOException {
        checkArgument(fieldNameAndTypes.length > 0);
        File sourceFile = tmpFolder.newFile();
        Collections.shuffle(data);
        Files.write(sourceFile.toPath(), String.join("\n", data).getBytes());
        String srcTableDdl =
                String.format(
                        "CREATE TABLE %s (\n"
                                + "%s\n"
                                + ") with (\n"
                                + "  'connector' = 'filesystem',\n"
                                + "  'path' = '%s',\n"
                                + "  'format' = 'testcsv')",
                        tableName,
                        String.join(",\n", fieldNameAndTypes),
                        sourceFile.getAbsolutePath());
        tableEnv.executeSql(srcTableDdl);
    }

    protected File createTestCsvSinkTable(String tableName, String... fieldNameAndTypes)
            throws IOException {
        checkArgument(fieldNameAndTypes.length > 0);
        File sinkPath = tmpFolder.newFolder();
        String srcTableDdl =
                String.format(
                        "CREATE TABLE %s (\n"
                                + "%s\n"
                                + ") with (\n"
                                + "  'connector' = 'filesystem',\n"
                                + "  'path' = '%s',\n"
                                + "  'format' = 'testcsv')",
                        tableName,
                        String.join(",\n", fieldNameAndTypes),
                        sinkPath.getAbsolutePath());
        tableEnv.executeSql(srcTableDdl);
        return sinkPath;
    }

    protected void assertResult(List<String> expected, File resultFile) throws IOException {
        List<String> actual = readLines(resultFile);
        Collections.sort(expected);
        Collections.sort(actual);
        assertEquals(expected, actual);
    }

    protected List<String> readLines(File path) throws IOException {
        List<String> result = new ArrayList<>();
        for (File file : checkNotNull(path.listFiles())) {
            if (file.isFile()) {
                String value = new String(Files.readAllBytes(file.toPath()));
                result.addAll(Arrays.asList(value.split("\n")));
            }
        }
        return result;
    }
}
