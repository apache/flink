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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;

import static org.apache.flink.table.factories.TestFormatFactory.CHANGELOG_MODE;

/** {@link TableTestProgram} definitions for testing {@link StreamExecRank}. */
public class RankTestPrograms {

    static final TableTestProgram RANK_TEST_APPEND_FAST_STRATEGY =
            getTableTestProgram(
                    "rank-test-append-fast-strategy",
                    "I",
                    new String[] {
                        "+I[2, a, 1]",
                        "+I[4, b, 1]",
                        "+I[6, c, 1]",
                        "-U[2, a, 1]",
                        "+U[1, a, 1]",
                        "-U[4, b, 1]",
                        "+U[3, b, 1]",
                        "-U[6, c, 1]",
                        "+U[5, c, 1]"
                    },
                    new String[] {"+I[4, d, 1]", "+I[3, e, 1]"});

    static final TableTestProgram RANK_TEST_RETRACT_STRATEGY =
            getTableTestProgram(
                    "rank-test-retract-strategy",
                    "I,UA,UB",
                    new String[] {
                        "+I[2, a, 1]",
                        "+I[4, b, 1]",
                        "+I[6, c, 1]",
                        "-D[2, a, 1]",
                        "+I[1, a, 1]",
                        "-D[4, b, 1]",
                        "+I[3, b, 1]",
                        "-D[6, c, 1]",
                        "+I[5, c, 1]"
                    },
                    new String[] {"+I[4, d, 1]", "+I[3, e, 1]"});

    static final TableTestProgram RANK_TEST_UPDATE_FAST_STRATEGY =
            TableTestProgram.of("rank-test-update-fast-strategy", "validates rank exec node")
                    .setupTableSource(
                            SourceTestStep.newBuilder("MyTable")
                                    .addSchema(
                                            "a INT primary key not enforced", "b VARCHAR", "c INT")
                                    .addOption(CHANGELOG_MODE, "I")
                                    .producedBeforeRestore(
                                            Row.of(2, "a", 6),
                                            Row.of(4, "b", 8),
                                            Row.of(6, "c", 10),
                                            Row.of(1, "a", 5),
                                            Row.of(3, "b", 7),
                                            Row.of(5, "c", 9))
                                    .producedAfterRestore(Row.of(4, "d", 7), Row.of(0, "a", 8))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "a INT NOT NULL",
                                            "b STRING",
                                            "count_c BIGINT NOT NULL",
                                            "row_num BIGINT NOT NULL")
                                    .consumedBeforeRestore(
                                            "+I[2, a, 1, 1]",
                                            "+I[4, b, 1, 1]",
                                            "+I[6, c, 1, 1]",
                                            "-U[2, a, 1, 1]",
                                            "+U[1, a, 1, 1]",
                                            "+I[2, a, 1, 2]",
                                            "-U[4, b, 1, 1]",
                                            "+U[3, b, 1, 1]",
                                            "+I[4, b, 1, 2]",
                                            "-U[6, c, 1, 1]",
                                            "+U[5, c, 1, 1]",
                                            "+I[6, c, 1, 2]")
                                    .consumedAfterRestore(
                                            new String[] {
                                                "+I[4, d, 1, 1]",
                                                "-U[1, a, 1, 1]",
                                                "+U[0, a, 1, 1]",
                                                "-U[2, a, 1, 2]",
                                                "+U[1, a, 1, 2]",
                                                "+I[2, a, 1, 3]"
                                            })
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT * FROM ("
                                    + "SELECT a, b, count_c, ROW_NUMBER() "
                                    + "   OVER (PARTITION BY b ORDER BY count_c DESC, a ASC) AS row_num"
                                    + "   FROM ("
                                    + "       SELECT a, b, COUNT(*) AS count_c"
                                    + "       FROM MyTable"
                                    + "       GROUP BY a, b"
                                    + "   )"
                                    + ") WHERE row_num <= 10")
                    .build();

    private static TableTestProgram getTableTestProgram(
            final String name,
            final String changelogMode,
            final String[] resultsBeforeRestore,
            final String[] resultsAfterRestore) {
        return TableTestProgram.of(name, "validates rank exec node")
                .setupTableSource(
                        SourceTestStep.newBuilder("MyTable")
                                .addSchema("a INT", "b VARCHAR", "c INT primary key not enforced")
                                .addOption(CHANGELOG_MODE, changelogMode)
                                .producedBeforeRestore(
                                        Row.of(2, "a", 6),
                                        Row.of(4, "b", 8),
                                        Row.of(6, "c", 10),
                                        Row.of(1, "a", 5),
                                        Row.of(3, "b", 7),
                                        Row.of(5, "c", 9))
                                .producedAfterRestore(Row.of(4, "d", 7), Row.of(3, "e", 8))
                                .build())
                .setupTableSink(
                        SinkTestStep.newBuilder("sink_t")
                                .addSchema("a INT", "b VARCHAR", "c BIGINT")
                                .consumedBeforeRestore(resultsBeforeRestore)
                                .consumedAfterRestore(resultsAfterRestore)
                                .build())
                .runSql(
                        "insert into `sink_t` select * from "
                                + "(select a, b, row_number() over(partition by b order by c) as c from MyTable)"
                                + " where c = 1")
                .build();
    }

    static final TableTestProgram RANK_N_TEST =
            TableTestProgram.of("rank-n-test", "validates rank node can handle multiple outputs")
                    .setupTableSource(
                            SourceTestStep.newBuilder("MyTable1")
                                    .addSchema("a STRING", "b INT", "c INT", "t as proctime()")
                                    .producedBeforeRestore(
                                            Row.of("book", 1, 12),
                                            Row.of("book", 2, 19),
                                            Row.of("book", 4, 11),
                                            Row.of("fruit", 4, 33))
                                    .producedAfterRestore(
                                            Row.of("cereal", 6, 21),
                                            Row.of("cereal", 7, 23),
                                            Row.of("apple", 8, 31),
                                            Row.of("fruit", 9, 41))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("result1")
                                    .addSchema("a varchar", "b int", "c bigint")
                                    .consumedBeforeRestore(
                                            "+I[book, 1, 1]", "+I[book, 2, 2]", "+I[fruit, 4, 1]")
                                    .consumedAfterRestore(
                                            "+I[cereal, 6, 1]",
                                            "+I[cereal, 7, 2]",
                                            "+I[apple, 8, 1]",
                                            "+I[fruit, 9, 2]")
                                    .build())
                    .runSql(
                            "insert into `result1` select * from "
                                    + "(select a, b, row_number() over(partition by a order by t asc) as c from MyTable1)"
                                    + " where c <= 2")
                    .build();
}
