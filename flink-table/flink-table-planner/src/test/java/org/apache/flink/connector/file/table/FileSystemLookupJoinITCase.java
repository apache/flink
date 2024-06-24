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

package org.apache.flink.connector.file.table;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.catalog.CatalogPartitionImpl;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.planner.factories.utils.TestCollectionTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test lookup join of file system. */
public class FileSystemLookupJoinITCase {
    private static TableEnvironment tEnv;
    @TempDir private static File file;

    @BeforeEach
    public void setup() throws Exception {
        tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        // create probe table
        TestCollectionTableFactory.initData(
                Arrays.asList(
                        Row.of(1, "a"),
                        Row.of(1, "c"),
                        Row.of(2, "b"),
                        Row.of(2, "c"),
                        Row.of(3, "c"),
                        Row.of(4, "d")));
        tEnv.executeSql(
                "create table probe (x int,y string, p as proctime()) "
                        + "with ('connector'='COLLECTION','is-bounded' = 'false')");

        String filePath1 =
                createFileAndWriteData(
                        file, "00-00.tmp", Arrays.asList("1,a,10", "2,a,21", "2,b,22", "3,c,33"));
        String ddl1 =
                String.format(
                        "CREATE TABLE bounded_table (\n"
                                + "  x int,\n"
                                + "  y string,\n"
                                + "  z int\n"
                                + ") with (\n"
                                + " 'connector' = 'filesystem',"
                                + " 'format' = 'testcsv',"
                                + " 'lookup.full-cache.periodic-reload.interval' = '10s',"
                                + " 'path' = '%s')",
                        filePath1);
        tEnv.executeSql(ddl1);

        File partitionDataPath = new File(file, "partitionData");
        partitionDataPath.mkdirs();
        writeData(new File(partitionDataPath, "z=10"), Arrays.asList("1,a,10", "2,b,10"));
        writeData(new File(partitionDataPath, "z=21"), Collections.singletonList("2,a,21"));
        writeData(new File(partitionDataPath, "z=22"), Arrays.asList("2,b,22", "3,c,22"));
        String ddl2 =
                String.format(
                        "CREATE TABLE bounded_partition_table (\n"
                                + "  x int,\n"
                                + "  y string,\n"
                                + "  z int \n"
                                + ") partitioned by(z) with (\n"
                                + " 'connector' = 'filesystem',"
                                + " 'format' = 'testcsv',"
                                + " 'lookup.full-cache.periodic-reload.interval' = '10s',"
                                + " 'path' = '%s')",
                        partitionDataPath.toURI());
        tEnv.executeSql(ddl2);
        tEnv.getCatalog(tEnv.getCurrentCatalog())
                .orElseThrow(Exception::new)
                .createPartition(
                        new ObjectPath(tEnv.getCurrentDatabase(), "bounded_partition_table"),
                        new CatalogPartitionSpec(Collections.singletonMap("z", "10")),
                        new CatalogPartitionImpl(new HashMap<>(), ""),
                        false);
        tEnv.getCatalog(tEnv.getCurrentCatalog())
                .orElseThrow(Exception::new)
                .createPartition(
                        new ObjectPath(tEnv.getCurrentDatabase(), "bounded_partition_table"),
                        new CatalogPartitionSpec(Collections.singletonMap("z", "21")),
                        new CatalogPartitionImpl(new HashMap<>(), ""),
                        false);
        tEnv.getCatalog(tEnv.getCurrentCatalog())
                .orElseThrow(Exception::new)
                .createPartition(
                        new ObjectPath(tEnv.getCurrentDatabase(), "bounded_partition_table"),
                        new CatalogPartitionSpec(Collections.singletonMap("z", "22")),
                        new CatalogPartitionImpl(new HashMap<>(), ""),
                        false);
    }

    @Test
    public void testLookupJoinBoundedTable() {
        TableImpl flinkTable =
                (TableImpl)
                        tEnv.sqlQuery(
                                "select p.x, p.y, b.z from "
                                        + " probe as p "
                                        + " join bounded_table for system_time as of p.p as b on p.x=b.x");
        List<Row> rows = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        assertThat(rows.toString())
                .isEqualTo(
                        "[+I[1, a, 10], +I[1, c, 10], +I[2, b, 21], +I[2, b, 22], +I[2, c, 21], +I[2, c, 22], +I[3, c, 33]]");
    }

    @Test
    public void testLookupJoinBoundedPartitionedTable() {
        TableImpl flinkTable =
                (TableImpl)
                        tEnv.sqlQuery(
                                "select p.x, p.y, b.z from "
                                        + " probe as p"
                                        + " join bounded_partition_table for system_time as of p.p as b on p.x=b.x where p.y = 'a'");
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        assertThat(results.toString()).isEqualTo("[+I[1, a, 10]]");
    }

    private void writeData(File file, List<String> data) throws IOException {
        Files.write(file.toPath(), String.join("\n", data).getBytes());
    }

    private String createFileAndWriteData(File path, String fileName, List<String> data)
            throws IOException {
        String file = path.getAbsolutePath() + "/" + fileName;
        Files.write(new File(file).toPath(), String.join("\n", data).getBytes());
        return file;
    }
}
