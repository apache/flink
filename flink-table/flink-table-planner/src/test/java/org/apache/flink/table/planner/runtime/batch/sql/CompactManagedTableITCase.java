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

package org.apache.flink.table.planner.runtime.batch.sql;

import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.planner.runtime.utils.BatchTestBase;
import org.apache.flink.table.utils.PartitionPathUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.apache.flink.table.factories.TestManagedTableFactory.MANAGED_TABLES;
import static org.apache.flink.table.factories.TestManagedTableFactory.MANAGED_TABLE_FILE_ENTRIES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/** IT Case for testing managed table compaction. */
public class CompactManagedTableITCase extends BatchTestBase {

    private final ObjectIdentifier tableIdentifier =
            ObjectIdentifier.of(tEnv().getCurrentCatalog(), tEnv().getCurrentDatabase(), "MyTable");
    private final Map<CatalogPartitionSpec, List<RowData>> collectedElements = new HashMap<>();

    private Path rootPath;
    private AtomicReference<Map<CatalogPartitionSpec, List<Path>>>
            referenceOfManagedTableFileEntries;

    @Override
    @Before
    public void before() throws Exception {
        super.before();
        MANAGED_TABLES.put(tableIdentifier, new AtomicReference<>());
        referenceOfManagedTableFileEntries = new AtomicReference<>();
        MANAGED_TABLE_FILE_ENTRIES.put(tableIdentifier, referenceOfManagedTableFileEntries);
        try {
            rootPath =
                    new Path(
                            new Path(createTempFolder().getPath()),
                            tableIdentifier.asSummaryString());
            rootPath.getFileSystem().mkdirs(rootPath);
        } catch (IOException e) {
            fail(String.format("Failed to create dir for %s", rootPath), e);
        }
    }

    @Override
    @After
    public void after() {
        super.after();
        tEnv().executeSql("DROP TABLE MyTable");
        collectedElements.clear();
        try {
            rootPath.getFileSystem().delete(rootPath, true);
        } catch (IOException e) {
            fail(String.format("Failed to delete dir for %s", rootPath), e);
        }
    }

    @Test
    public void testCompactPartitionOnNonPartitionedTable() {
        String sql = "CREATE TABLE MyTable (id BIGINT, content STRING)";
        tEnv().executeSql(sql);
        assertThatThrownBy(
                        () ->
                                tEnv().executeSql(
                                                "ALTER TABLE MyTable PARTITION (season = 'summer') COMPACT"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        String.format("Table %s is not partitioned.", tableIdentifier));
    }

    @Test
    public void testCompactPartitionOnNonExistedPartitionKey() {
        String sql =
                "CREATE TABLE MyTable (\n"
                        + "  id BIGINT,\n"
                        + "  content STRING,\n"
                        + "  season STRING\n"
                        + ") PARTITIONED BY (season)";
        tEnv().executeSql(sql);
        assertThatThrownBy(
                        () ->
                                tEnv().executeSql(
                                                "ALTER TABLE MyTable PARTITION (saeson = 'summer') COMPACT"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Partition column 'saeson' not defined in the table schema. "
                                + "Available ordered partition columns: ['season']");
    }

    @Test
    public void testCompactPartitionOnNonExistedPartitionValue() throws Exception {
        String sql =
                "CREATE TABLE MyTable (\n"
                        + "  id BIGINT,\n"
                        + "  content STRING,\n"
                        + "  season STRING\n"
                        + ") PARTITIONED BY (season)";
        prepare(sql, Collections.singletonList(of("season", "'spring'")));
        assertThatThrownBy(
                        () ->
                                tEnv().executeSql(
                                                "ALTER TABLE MyTable PARTITION (season = 'summer') COMPACT"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Cannot resolve partition spec CatalogPartitionSpec{{season=summer}}");
    }

    @Test
    public void testCompactNonPartitionedTable() throws Exception {
        String sql = "CREATE TABLE MyTable (id BIGINT, content STRING)";
        prepare(sql, Collections.emptyList());

        // test compact table
        CatalogPartitionSpec unresolvedDummySpec = new CatalogPartitionSpec(Collections.emptyMap());
        Set<CatalogPartitionSpec> resolvedPartitionSpecsHaveBeenOrToBeCompacted =
                Collections.singleton(unresolvedDummySpec);
        executeAndCheck(unresolvedDummySpec, resolvedPartitionSpecsHaveBeenOrToBeCompacted);
    }

    @Test
    public void testCompactSinglePartitionedTable() throws Exception {
        String sql =
                "CREATE TABLE MyTable (\n"
                        + "  id BIGINT,\n"
                        + "  content STRING,\n"
                        + "  season STRING\n"
                        + ") PARTITIONED BY (season)";
        prepare(sql, Arrays.asList(of("season", "'spring'"), of("season", "'summer'")));

        Set<CatalogPartitionSpec> resolvedPartitionSpecsHaveBeenOrToBeCompacted = new HashSet<>();

        // test compact one partition
        CatalogPartitionSpec unresolvedPartitionSpec =
                new CatalogPartitionSpec(of("season", "'summer'"));
        resolvedPartitionSpecsHaveBeenOrToBeCompacted.add(
                new CatalogPartitionSpec(of("season", "summer")));
        executeAndCheck(unresolvedPartitionSpec, resolvedPartitionSpecsHaveBeenOrToBeCompacted);

        // test compact the whole table
        unresolvedPartitionSpec = new CatalogPartitionSpec(Collections.emptyMap());
        resolvedPartitionSpecsHaveBeenOrToBeCompacted.add(
                new CatalogPartitionSpec(of("season", "spring")));
        executeAndCheck(unresolvedPartitionSpec, resolvedPartitionSpecsHaveBeenOrToBeCompacted);
    }

    @Test
    public void testCompactMultiPartitionedTable() throws Exception {
        String sql =
                "CREATE TABLE MyTable ("
                        + "  id BIGINT,\n"
                        + "  content STRING,\n"
                        + "  season STRING,\n"
                        + "  `month` INT\n"
                        + ") PARTITIONED BY (season, `month`)";

        prepare(
                sql,
                Arrays.asList(
                        // spring
                        of("season", "'spring'", "`month`", "2"),
                        of("season", "'spring'", "`month`", "3"),
                        of("season", "'spring'", "`month`", "4"),
                        // summer
                        of("season", "'summer'", "`month`", "5"),
                        of("season", "'summer'", "`month`", "6"),
                        of("season", "'summer'", "`month`", "7"),
                        of("season", "'summer'", "`month`", "8"),
                        // autumn
                        of("season", "'autumn'", "`month`", "8"),
                        of("season", "'autumn'", "`month`", "9"),
                        of("season", "'autumn'", "`month`", "10"),
                        // winter
                        of("season", "'winter'", "`month`", "11"),
                        of("season", "'winter'", "`month`", "12"),
                        of("season", "'winter'", "`month`", "1")));

        Set<CatalogPartitionSpec> resolvedPartitionSpecsHaveBeenOrToBeCompacted = new HashSet<>();

        // test compact one partition with full ordered partition spec
        CatalogPartitionSpec unresolvedPartitionSpec =
                new CatalogPartitionSpec(of("season", "'spring'", "`month`", "2"));
        resolvedPartitionSpecsHaveBeenOrToBeCompacted.add(
                new CatalogPartitionSpec(of("season", "spring", "month", "2")));
        executeAndCheck(unresolvedPartitionSpec, resolvedPartitionSpecsHaveBeenOrToBeCompacted);

        // test compact one partition with full but disordered partition spec
        unresolvedPartitionSpec =
                new CatalogPartitionSpec(of("`month`", "3", "season", "'spring'"));
        resolvedPartitionSpecsHaveBeenOrToBeCompacted.add(
                new CatalogPartitionSpec(of("season", "spring", "month", "3")));
        executeAndCheck(unresolvedPartitionSpec, resolvedPartitionSpecsHaveBeenOrToBeCompacted);

        // test compact multiple partitions with the subordinate partition spec
        unresolvedPartitionSpec = new CatalogPartitionSpec(of("season", "'winter'"));
        resolvedPartitionSpecsHaveBeenOrToBeCompacted.add(
                new CatalogPartitionSpec(of("season", "winter", "month", "1")));
        resolvedPartitionSpecsHaveBeenOrToBeCompacted.add(
                new CatalogPartitionSpec(of("season", "winter", "month", "11")));
        resolvedPartitionSpecsHaveBeenOrToBeCompacted.add(
                new CatalogPartitionSpec(of("season", "winter", "month", "12")));
        executeAndCheck(unresolvedPartitionSpec, resolvedPartitionSpecsHaveBeenOrToBeCompacted);

        // test compact one partition with the secondary partition spec
        unresolvedPartitionSpec = new CatalogPartitionSpec(of("`month`", "5"));
        resolvedPartitionSpecsHaveBeenOrToBeCompacted.add(
                new CatalogPartitionSpec(of("season", "summer", "month", "5")));
        executeAndCheck(unresolvedPartitionSpec, resolvedPartitionSpecsHaveBeenOrToBeCompacted);

        // test compact multiple partitions with the secondary partition spec
        unresolvedPartitionSpec = new CatalogPartitionSpec(of("`month`", "8"));
        resolvedPartitionSpecsHaveBeenOrToBeCompacted.add(
                new CatalogPartitionSpec(of("season", "summer", "month", "8")));
        resolvedPartitionSpecsHaveBeenOrToBeCompacted.add(
                new CatalogPartitionSpec(of("season", "autumn", "month", "8")));
        executeAndCheck(unresolvedPartitionSpec, resolvedPartitionSpecsHaveBeenOrToBeCompacted);

        // test compact the whole table
        unresolvedPartitionSpec = new CatalogPartitionSpec(Collections.emptyMap());
        resolvedPartitionSpecsHaveBeenOrToBeCompacted.add(
                new CatalogPartitionSpec(of("season", "spring", "month", "4")));
        resolvedPartitionSpecsHaveBeenOrToBeCompacted.add(
                new CatalogPartitionSpec(of("season", "summer", "month", "6")));
        resolvedPartitionSpecsHaveBeenOrToBeCompacted.add(
                new CatalogPartitionSpec(of("season", "summer", "month", "7")));
        resolvedPartitionSpecsHaveBeenOrToBeCompacted.add(
                new CatalogPartitionSpec(of("season", "autumn", "month", "9")));
        resolvedPartitionSpecsHaveBeenOrToBeCompacted.add(
                new CatalogPartitionSpec(of("season", "autumn", "month", "10")));
        executeAndCheck(unresolvedPartitionSpec, resolvedPartitionSpecsHaveBeenOrToBeCompacted);
    }

    // ~ Tools ------------------------------------------------------------------

    private void prepare(String managedTableDDL, List<LinkedHashMap<String, String>> partitionKVs)
            throws Exception {
        prepareMirrorTables(managedTableDDL);
        prepareFileEntries(partitionKVs);
        scanFileEntries();
    }

    private void prepareMirrorTables(String managedTableDDL) {
        tEnv().executeSql(managedTableDDL);
        String helperSource =
                "CREATE TABLE HelperSource (id BIGINT, content STRING ) WITH ("
                        + "  'connector' = 'datagen', "
                        + "  'rows-per-second' = '5', "
                        + "  'fields.id.kind' = 'sequence', "
                        + "  'fields.id.start' = '0', "
                        + "  'fields.id.end' = '200', "
                        + "  'fields.content.kind' = 'random', "
                        + "  'number-of-rows' = '50')";
        String helperSink =
                String.format(
                        "CREATE TABLE HelperSink WITH ("
                                + "  'connector' = 'filesystem', "
                                + "  'format' = 'testcsv', "
                                + "  'path' = '%s' )"
                                + "LIKE MyTable (EXCLUDING OPTIONS)",
                        rootPath.getPath());

        tEnv().executeSql(helperSource);
        tEnv().executeSql(helperSink);
    }

    private void prepareFileEntries(List<LinkedHashMap<String, String>> partitionKVs)
            throws Exception {
        tEnv().executeSql(prepareInsertDML(partitionKVs)).await();
    }

    private static String prepareInsertDML(List<LinkedHashMap<String, String>> partitionKVs) {
        StringBuilder dmlBuilder = new StringBuilder("INSERT INTO HelperSink\n");
        if (partitionKVs.isEmpty()) {
            return dmlBuilder.append("SELECT id,\n  content\nFROM HelperSource\n").toString();
        }

        for (int i = 0; i < partitionKVs.size(); i++) {
            dmlBuilder.append("SELECT id,\n  content,\n");
            int j = 0;
            for (Map.Entry<String, String> entry : partitionKVs.get(i).entrySet()) {
                dmlBuilder.append("  ");
                dmlBuilder.append(entry.getValue());
                dmlBuilder.append(" AS ");
                dmlBuilder.append(entry.getKey());
                if (j < partitionKVs.get(i).size() - 1) {
                    dmlBuilder.append(",\n");
                } else {
                    dmlBuilder.append("\n");
                }
                j++;
            }
            dmlBuilder.append("FROM HelperSource\n");
            if (i < partitionKVs.size() - 1) {
                dmlBuilder.append("UNION ALL\n");
            }
        }
        return dmlBuilder.toString();
    }

    private void scanFileEntries() throws IOException {
        Map<CatalogPartitionSpec, List<Path>> managedTableFileEntries = new HashMap<>();
        try (Stream<java.nio.file.Path> pathStream = Files.walk(Paths.get(rootPath.getPath()))) {
            pathStream
                    .filter(Files::isRegularFile)
                    .forEach(
                            filePath -> {
                                Path file = new Path(filePath.toString());
                                CatalogPartitionSpec partitionSpec =
                                        new CatalogPartitionSpec(
                                                PartitionPathUtils.extractPartitionSpecFromPath(
                                                        file));
                                // for non-partitioned table, the map is empty
                                List<Path> fileEntries =
                                        managedTableFileEntries.getOrDefault(
                                                partitionSpec, new ArrayList<>());
                                fileEntries.add(file);
                                managedTableFileEntries.put(partitionSpec, fileEntries);

                                List<RowData> elements =
                                        collectedElements.getOrDefault(
                                                partitionSpec, new ArrayList<>());
                                elements.addAll(readElementsFromFile(filePath.toFile()));
                                collectedElements.put(partitionSpec, elements);
                            });
        }
        referenceOfManagedTableFileEntries.set(managedTableFileEntries);
    }

    private static List<RowData> readElementsFromFile(File file) {
        List<RowData> elements = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                elements.add(GenericRowData.of(StringData.fromString(line)));
            }
        } catch (IOException e) {
            fail("This should not happen");
        }
        return elements;
    }

    private LinkedHashMap<String, String> of(String... kvs) {
        assertThat(kvs != null && kvs.length % 2 == 0).isTrue();
        LinkedHashMap<String, String> map = new LinkedHashMap<>();
        for (int i = 0; i < kvs.length - 1; i += 2) {
            map.put(kvs[i], kvs[i + 1]);
        }
        return map;
    }

    private static String prepareCompactSql(CatalogPartitionSpec unresolvedCompactPartitionSpec) {
        String compactSqlTemplate = "ALTER TABLE MyTable%s COMPACT";
        Map<String, String> partitionKVs = unresolvedCompactPartitionSpec.getPartitionSpec();
        StringBuilder sb = new StringBuilder();
        int index = 0;
        for (Map.Entry<String, String> entry : partitionKVs.entrySet()) {
            if (index == 0) {
                sb.append(" PARTITION (");
            }
            sb.append(entry.getKey());
            sb.append(" = ");
            sb.append(entry.getValue());
            if (index < partitionKVs.size() - 1) {
                sb.append(", ");
            }
            if (index == partitionKVs.size() - 1) {
                sb.append(")");
            }
            index++;
        }
        return String.format(compactSqlTemplate, sb);
    }

    private void executeAndCheck(
            CatalogPartitionSpec unresolvedPartitionSpec,
            Set<CatalogPartitionSpec> resolvedPartitionSpecsHaveBeenOrToBeCompacted)
            throws ExecutionException, InterruptedException {
        String compactSql = prepareCompactSql(unresolvedPartitionSpec);

        // first run to check compacted file size and content
        tEnv().executeSql(compactSql).await();
        Map<CatalogPartitionSpec, Long> firstRun =
                checkFileAndElements(resolvedPartitionSpecsHaveBeenOrToBeCompacted);

        // second run to check idempotence
        tEnv().executeSql(compactSql).await();
        Map<CatalogPartitionSpec, Long> secondRun =
                checkFileAndElements(resolvedPartitionSpecsHaveBeenOrToBeCompacted);
        checkModifiedTime(firstRun, secondRun);
    }

    private Map<CatalogPartitionSpec, Long> checkFileAndElements(
            Set<CatalogPartitionSpec> resolvedPartitionSpecsHaveBeenOrToBeCompacted) {
        Map<CatalogPartitionSpec, Long> lastModifiedForEachPartition = new HashMap<>();
        Map<CatalogPartitionSpec, List<Path>> managedTableFileEntries =
                referenceOfManagedTableFileEntries.get();
        managedTableFileEntries.forEach(
                (partitionSpec, fileEntries) -> {
                    if (resolvedPartitionSpecsHaveBeenOrToBeCompacted.contains(partitionSpec)) {
                        assertThat(fileEntries).hasSize(1);
                        Path compactedFile = fileEntries.get(0);
                        assertThat(compactedFile.getName()).startsWith("compact-");
                        List<RowData> compactedElements =
                                readElementsFromFile(new File(compactedFile.getPath()));
                        assertThat(compactedElements)
                                .hasSameElementsAs(collectedElements.get(partitionSpec));
                        lastModifiedForEachPartition.put(
                                partitionSpec, getLastModifiedTime(compactedFile));
                    } else {
                        // check remaining partitions are untouched
                        fileEntries.forEach(
                                file -> {
                                    assertThat(file.getName()).startsWith("part-");
                                    List<RowData> elements =
                                            readElementsFromFile(new File(file.getPath()));
                                    assertThat(collectedElements.get(partitionSpec))
                                            .containsAll(elements);
                                });
                    }
                });
        return lastModifiedForEachPartition;
    }

    private void checkModifiedTime(
            Map<CatalogPartitionSpec, Long> firstRun, Map<CatalogPartitionSpec, Long> secondRun) {
        firstRun.forEach(
                (partitionSpec, lastModified) ->
                        assertThat(secondRun.get(partitionSpec))
                                .isEqualTo(lastModified)
                                .isNotEqualTo(-1L));
    }

    private static long getLastModifiedTime(Path compactedFile) {
        try {
            FileStatus status = compactedFile.getFileSystem().getFileStatus(compactedFile);
            return status.getModificationTime();
        } catch (IOException e) {
            fail("This should not happen");
        }
        return -1L;
    }
}
