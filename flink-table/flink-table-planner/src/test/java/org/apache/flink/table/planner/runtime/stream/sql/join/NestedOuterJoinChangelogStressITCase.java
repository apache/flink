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

package org.apache.flink.table.planner.runtime.stream.sql.join;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Differential test for the nested outer join query reported in FLINK-23740, see FLINK-40681.
 * Seeded random upsert changelogs for the four tables are joined in streaming mode into an upsert
 * sink keyed on A's primary key; the materialized result must equal the same query executed in
 * batch mode over the final snapshot of every table.
 *
 * <p>The FULL OUTER JOIN variant (the reported query) keeps UPDATE_BEFORE because its upsert key
 * differs from the sink key. The LEFT OUTER JOIN variant runs without UPDATE_BEFORE and hits the
 * association counter defect of {@code StreamingJoinOperator}.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class NestedOuterJoinChangelogStressITCase extends StreamingWithStateTestBase {

    private static final int SEEDS = 20;
    private static final int OPS_PER_TABLE = 60;
    private static final int KEY_SPACE = 3;

    public NestedOuterJoinChangelogStressITCase(StateBackendMode state) {
        super(state);
    }

    @BeforeEach
    public void before() {
        super.before();
        env().setParallelism(1);
    }

    @TestTemplate
    void testFullVariantMatchesBatch() throws Exception {
        runSeeds("FULL OUTER JOIN");
    }

    @TestTemplate
    void testLeftVariantMatchesBatch() throws Exception {
        runSeeds("LEFT OUTER JOIN");
    }

    // ------------------------------------------------------------------------------------------

    private static final class TableSpec {
        final String name;
        final String[] keyCols;
        final String payload;

        TableSpec(String name, String[] keyCols, String payload) {
            this.name = name;
            this.keyCols = keyCols;
            this.payload = payload;
        }

        String columns() {
            return Arrays.stream(keyCols)
                            .map(c -> c + " INT NOT NULL")
                            .collect(Collectors.joining(", "))
                    + ", "
                    + payload
                    + " STRING, PRIMARY KEY ("
                    + String.join(", ", keyCols)
                    + ") NOT ENFORCED";
        }
    }

    private static final TableSpec A =
            new TableSpec("A", new String[] {"k1", "k2", "k3", "k4", "k5"}, "a");
    private static final TableSpec B = new TableSpec("B", new String[] {"k1", "k2", "k3"}, "b");
    private static final TableSpec C = new TableSpec("C", new String[] {"k1", "k2", "k3"}, "c");
    private static final TableSpec D = new TableSpec("D", new String[] {"k1", "k2"}, "d");

    private void runSeeds(String innerJoin) throws Exception {
        final List<String> failures = new ArrayList<>();
        for (int seed = 0; seed < SEEDS; seed++) {
            final String failure = runSeed(seed, innerJoin);
            if (failure != null) {
                failures.add(failure);
            }
            TestValuesTableFactory.clearAllData();
        }
        assertThat(failures)
                .as(
                        "%s: %d of %d seeds diverged from the batch result",
                        innerJoin, failures.size(), SEEDS)
                .isEmpty();
    }

    /** Returns null on success, otherwise a description of the divergence. */
    private String runSeed(int seed, String innerJoin) throws Exception {
        final Random random = new Random(seed);
        final Map<TableSpec, List<Row>> changelogs = new LinkedHashMap<>();
        final Map<TableSpec, List<Row>> snapshots = new LinkedHashMap<>();
        for (TableSpec spec : Arrays.asList(A, B, C, D)) {
            final Map<List<Integer>, String> model = new LinkedHashMap<>();
            final List<Row> changelog = new ArrayList<>();
            for (int op = 0; op < OPS_PER_TABLE; op++) {
                final List<Integer> key = new ArrayList<>();
                for (int i = 0; i < spec.keyCols.length; i++) {
                    key.add(1 + random.nextInt(KEY_SPACE));
                }
                final String value = spec.payload + seed + "_" + op;
                final boolean present = model.containsKey(key);
                final int choice = random.nextInt(3);
                if (!present || choice == 0) {
                    model.put(key, value);
                    changelog.add(row(present ? RowKind.UPDATE_AFTER : RowKind.INSERT, key, value));
                } else if (choice == 1) {
                    model.remove(key);
                    changelog.add(row(RowKind.DELETE, key, value));
                } else {
                    model.put(key, value);
                    changelog.add(row(RowKind.UPDATE_AFTER, key, value));
                }
            }
            changelogs.put(spec, changelog);
            final List<Row> snapshot = new ArrayList<>();
            model.forEach((k, v) -> snapshot.add(row(RowKind.INSERT, k, v)));
            snapshots.put(spec, snapshot);
        }

        final String suffix = "_" + seed;
        final String sink = "sink" + suffix;
        // streaming
        for (TableSpec spec : changelogs.keySet()) {
            final String id = TestValuesTableFactory.registerData(changelogs.get(spec));
            tEnv().executeSql(
                            String.format(
                                    "CREATE TABLE %s (%s) WITH ('connector' = 'values', 'data-id' = '%s',"
                                            + " 'changelog-mode' = 'I,UA,D')",
                                    spec.name + suffix, spec.columns(), id));
        }
        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE %s (k1 INT NOT NULL, k2 INT NOT NULL, k3 INT NOT NULL,"
                                        + " k4 INT NOT NULL, k5 INT NOT NULL, a STRING, b STRING, c STRING,"
                                        + " d STRING, PRIMARY KEY (k1, k2, k3, k4, k5) NOT ENFORCED)"
                                        + " WITH ('connector' = 'values', 'sink-insert-only' = 'false')",
                                sink));
        final String select = query(innerJoin, suffix);
        try {
            tEnv().executeSql("INSERT INTO " + sink + " " + select + " ON CONFLICT DO DEDUPLICATE")
                    .await();
        } catch (Exception e) {
            Throwable root = e;
            while (root.getCause() != null) {
                root = root.getCause();
            }
            return String.format(
                    "seed %d: streaming job failed: %s%nchangelogs=%s",
                    seed, root.getMessage(), changelogs);
        }
        final List<String> streaming =
                new ArrayList<>(TestValuesTableFactory.getResultsAsStrings(sink));
        Collections.sort(streaming);

        // batch oracle over the final snapshots
        final TableEnvironment batchEnv =
                TableEnvironment.create(EnvironmentSettings.inBatchMode());
        for (TableSpec spec : snapshots.keySet()) {
            final String id = TestValuesTableFactory.registerData(snapshots.get(spec));
            batchEnv.executeSql(
                    String.format(
                            "CREATE TABLE %s (%s) WITH ('connector' = 'values', 'data-id' = '%s',"
                                    + " 'bounded' = 'true')",
                            spec.name + suffix, spec.columns(), id));
        }
        final List<String> batch = new ArrayList<>();
        try (CloseableIterator<Row> it = batchEnv.executeSql(select).collect()) {
            while (it.hasNext()) {
                batch.add(it.next().toString());
            }
        }
        Collections.sort(batch);

        if (!streaming.equals(batch)) {
            return String.format(
                    "seed %d: streaming=%s%nbatch=%s%nchangelogs=%s",
                    seed, streaming, batch, changelogs);
        }
        return null;
    }

    private static String query(String innerJoin, String suffix) {
        return String.format(
                "SELECT A.k1, A.k2, A.k3, A.k4, A.k5, A.a, BC.b, BC.c, D.d\n"
                        + "FROM A%1$s AS A\n"
                        + "LEFT OUTER JOIN (\n"
                        + "  SELECT B.k1, B.k2, B.k3, B.b, C.c\n"
                        + "  FROM B%1$s AS B %2$s C%1$s AS C\n"
                        + "  ON B.k1 = C.k1 AND B.k2 = C.k2 AND B.k3 = C.k3\n"
                        + ") AS BC ON A.k1 = BC.k1 AND A.k2 = BC.k2 AND A.k3 = BC.k3\n"
                        + "LEFT OUTER JOIN D%1$s AS D ON A.k1 = D.k1 AND A.k2 = D.k2",
                suffix, innerJoin);
    }

    private static Row row(RowKind kind, List<Integer> key, String value) {
        final Object[] fields = new Object[key.size() + 1];
        for (int i = 0; i < key.size(); i++) {
            fields[i] = key.get(i);
        }
        fields[key.size()] = value;
        return Row.ofKind(kind, fields);
    }
}
