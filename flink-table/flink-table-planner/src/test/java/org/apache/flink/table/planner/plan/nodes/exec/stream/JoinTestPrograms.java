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

/** {@link TableTestProgram} definitions for testing {@link StreamExecJoin}. */
public class JoinTestPrograms {
    static final TableTestProgram NON_WINDOW_INNER_JOIN;
    static final TableTestProgram NON_WINDOW_INNER_JOIN_WITH_NULL;
    static final TableTestProgram CROSS_JOIN;
    static final TableTestProgram JOIN_WITH_FILTER;
    static final TableTestProgram INNER_JOIN_WITH_DUPLICATE_KEY;
    static final TableTestProgram INNER_JOIN_WITH_NON_EQUI_JOIN;
    static final TableTestProgram INNER_JOIN_WITH_EQUAL_PK;
    static final TableTestProgram INNER_JOIN_WITH_PK;
    static final TableTestProgram LEFT_JOIN;
    static final TableTestProgram FULL_OUTER;
    static final TableTestProgram RIGHT_JOIN;
    static final TableTestProgram SEMI_JOIN;
    static final TableTestProgram ANTI_JOIN;
    static final TableTestProgram JOIN_WITH_STATE_TTL_HINT;

    static final SourceTestStep EMPLOYEE =
            SourceTestStep.newBuilder("EMPLOYEE")
                    .addSchema("deptno int", "salary bigint", "name varchar")
                    .addOption("filterable-fields", "salary")
                    .producedBeforeRestore(
                            Row.of(null, 101L, "Adam"),
                            Row.of(1, 1L, "Baker"),
                            Row.of(2, 2L, "Charlie"),
                            Row.of(3, 2L, "Don"),
                            Row.of(7, 6L, "Victor"))
                    .producedAfterRestore(
                            Row.of(4, 3L, "Juliet"),
                            Row.of(4, 4L, "Helena"),
                            Row.of(1, 1L, "Ivana"))
                    .build();

    static final SourceTestStep DEPARTMENT =
            SourceTestStep.newBuilder("DEPARTMENT")
                    .addSchema(
                            "department_num int", "b2 bigint", "b3 int", "department_name varchar")
                    .producedBeforeRestore(
                            Row.of(null, 102L, 0, "Accounting"),
                            Row.of(1, 1L, 0, "Research"),
                            Row.of(2, 2L, 1, "Human Resources"),
                            Row.of(2, 3L, 2, "HR"),
                            Row.of(3, 1L, 2, "Sales"))
                    .producedAfterRestore(
                            Row.of(2, 4L, 3, "People Operations"), Row.of(4, 2L, 4, "Engineering"))
                    .build();

    static final SourceTestStep DEPARTMENT_NONULLS =
            SourceTestStep.newBuilder("DEPARTMENT")
                    .addSchema(
                            "department_num int", "b2 bigint", "b3 int", "department_name varchar")
                    .producedBeforeRestore(
                            Row.of(1, 1L, 0, "Research"),
                            Row.of(2, 2L, 1, "Human Resources"),
                            Row.of(2, 3L, 2, "HR"),
                            Row.of(3, 1L, 2, "Sales"))
                    .producedAfterRestore(Row.of(2, 4L, 3, "People Operations"))
                    .build();
    static final SourceTestStep SOURCE_T1 =
            SourceTestStep.newBuilder("T1")
                    .addSchema("a int", "b bigint", "c varchar")
                    .producedBeforeRestore(
                            Row.of(1, 1L, "Baker1"),
                            Row.of(1, 2L, "Baker2"),
                            Row.of(1, 2L, "Baker2"),
                            Row.of(1, 5L, "Baker3"),
                            Row.of(2, 7L, "Baker5"),
                            Row.of(1, 9L, "Baker6"),
                            Row.of(1, 8L, "Baker8"),
                            Row.of(3, 8L, "Baker9"))
                    .producedAfterRestore(Row.of(1, 1L, "PostRestore"))
                    .build();
    static final SourceTestStep SOURCE_T2 =
            SourceTestStep.newBuilder("T2")
                    .addSchema("a int", "b bigint", "c varchar")
                    .producedBeforeRestore(
                            Row.of(1, 1L, "BakerBaker"),
                            Row.of(2, 2L, "HeHe"),
                            Row.of(3, 2L, "HeHe"))
                    .producedAfterRestore(Row.of(2, 1L, "PostRestoreRight"))
                    .build();

    static {
        NON_WINDOW_INNER_JOIN =
                TableTestProgram.of("join-non-window-inner-join", "test non-window inner join")
                        .setupTableSource(SOURCE_T1)
                        .setupTableSource(SOURCE_T2)
                        .setupTableSink(
                                SinkTestStep.newBuilder("MySink")
                                        .addSchema("a int", "c1 varchar", "c2 varchar")
                                        .consumedBeforeRestore(
                                                Row.of(1, "BakerBaker", "Baker2"),
                                                Row.of(1, "BakerBaker", "Baker2"),
                                                Row.of(1, "BakerBaker", "Baker3"),
                                                Row.of(2, "HeHe", "Baker5"),
                                                Row.of(1, "BakerBaker", "Baker6"),
                                                Row.of(1, "BakerBaker", "Baker8"))
                                        .consumedAfterRestore(
                                                Row.of(2, "PostRestoreRight", "Baker5"))
                                        .build())
                        .runSql(
                                "insert into MySink "
                                        + "SELECT t2.a, t2.c, t1.c\n"
                                        + "FROM (\n"
                                        + " SELECT if(a = 3, cast(null as int), a) as a, b, c FROM T1\n"
                                        + ") as t1\n"
                                        + "JOIN (\n"
                                        + " SELECT if(a = 3, cast(null as int), a) as a, b, c FROM T2\n"
                                        + ") as t2\n"
                                        + "ON t1.a = t2.a AND t1.b > t2.b")
                        .build();

        NON_WINDOW_INNER_JOIN_WITH_NULL =
                TableTestProgram.of(
                                "join-non-window-inner-join-with-null-cond",
                                "test non-window inner join")
                        .setupTableSource(SOURCE_T1)
                        .setupTableSource(SOURCE_T2)
                        .setupTableSink(
                                SinkTestStep.newBuilder("MySink")
                                        .addSchema("a int", "c1 varchar", "c2 varchar")
                                        .consumedBeforeRestore(
                                                Row.of(1, "BakerBaker", "Baker2"),
                                                Row.of(1, "BakerBaker", "Baker2"),
                                                Row.of(1, "BakerBaker", "Baker3"),
                                                Row.of(2, "HeHe", "Baker5"),
                                                Row.of(1, "BakerBaker", "Baker6"),
                                                Row.of(1, "BakerBaker", "Baker8"),
                                                Row.of(null, "HeHe", "Baker9"))
                                        .consumedAfterRestore(
                                                Row.of(2, "PostRestoreRight", "Baker5"))
                                        .build())
                        .runSql(
                                "insert into MySink "
                                        + "SELECT t2.a, t2.c, t1.c\n"
                                        + "FROM (\n"
                                        + " SELECT if(a = 3, cast(null as int), a) as a, b, c FROM T1\n"
                                        + ") as t1\n"
                                        + "JOIN (\n"
                                        + " SELECT if(a = 3, cast(null as int), a) as a, b, c FROM T2\n"
                                        + ") as t2\n"
                                        + "ON \n"
                                        + "  ((t1.a is null AND t2.a is null) OR\n"
                                        + "  (t1.a = t2.a))\n"
                                        + "  AND t1.b > t2.b")
                        .build();

        CROSS_JOIN =
                TableTestProgram.of("join-cross-join", "test cross join")
                        .setupTableSource(EMPLOYEE)
                        .setupTableSource(DEPARTMENT)
                        .setupTableSink(
                                SinkTestStep.newBuilder("MySink")
                                        .addSchema("name varchar", "department_name varchar")
                                        .consumedBeforeRestore(
                                                Row.of("Adam", "Accounting"),
                                                Row.of("Baker", "Accounting"),
                                                Row.of("Adam", "Research"),
                                                Row.of("Baker", "Research"),
                                                Row.of("Charlie", "Accounting"),
                                                Row.of("Charlie", "Research"),
                                                Row.of("Charlie", "Human Resources"),
                                                Row.of("Adam", "Human Resources"),
                                                Row.of("Baker", "Human Resources"),
                                                Row.of("Don", "Accounting"),
                                                Row.of("Don", "Human Resources"),
                                                Row.of("Don", "Research"),
                                                Row.of("Victor", "Accounting"),
                                                Row.of("Victor", "Human Resources"),
                                                Row.of("Victor", "Research"),
                                                Row.of("Don", "HR"),
                                                Row.of("Charlie", "HR"),
                                                Row.of("Adam", "HR"),
                                                Row.of("Baker", "HR"),
                                                Row.of("Victor", "HR"),
                                                Row.of("Don", "Sales"),
                                                Row.of("Charlie", "Sales"),
                                                Row.of("Adam", "Sales"),
                                                Row.of("Baker", "Sales"),
                                                Row.of("Victor", "Sales"))
                                        .consumedAfterRestore(
                                                Row.of("Juliet", "Human Resources"),
                                                Row.of("Juliet", "Sales"),
                                                Row.of("Juliet", "Research"),
                                                Row.of("Juliet", "Accounting"),
                                                Row.of("Juliet", "HR"),
                                                Row.of("Juliet", "People Operations"),
                                                Row.of("Victor", "People Operations"),
                                                Row.of("Charlie", "People Operations"),
                                                Row.of("Baker", "People Operations"),
                                                Row.of("Adam", "People Operations"),
                                                Row.of("Don", "People Operations"),
                                                Row.of("Helena", "Accounting"),
                                                Row.of("Helena", "Human Resources"),
                                                Row.of("Helena", "HR"),
                                                Row.of("Helena", "People Operations"),
                                                Row.of("Helena", "Sales"),
                                                Row.of("Helena", "Research"),
                                                Row.of("Don", "Engineering"),
                                                Row.of("Adam", "Engineering"),
                                                Row.of("Victor", "Engineering"),
                                                Row.of("Baker", "Engineering"),
                                                Row.of("Charlie", "Engineering"),
                                                Row.of("Juliet", "Engineering"),
                                                Row.of("Helena", "Engineering"),
                                                Row.of("Ivana", "Accounting"),
                                                Row.of("Ivana", "Human Resources"),
                                                Row.of("Ivana", "HR"),
                                                Row.of("Ivana", "Engineering"),
                                                Row.of("Ivana", "People Operations"),
                                                Row.of("Ivana", "Sales"),
                                                Row.of("Ivana", "Research"))
                                        .build())
                        .runSql(
                                "insert into MySink "
                                        + "SELECT name, department_name FROM EMPLOYEE, DEPARTMENT")
                        .build();

        JOIN_WITH_FILTER =
                TableTestProgram.of("join-with-filter", "test join with filter")
                        .setupTableSource(EMPLOYEE)
                        .setupTableSource(DEPARTMENT)
                        .setupTableSink(
                                SinkTestStep.newBuilder("MySink")
                                        .addSchema("name varchar", "department_name varchar")
                                        .consumedBeforeRestore(
                                                Row.of("Baker", "Research"),
                                                Row.of("Baker", "Sales"))
                                        .consumedAfterRestore(
                                                Row.of("Ivana", "Sales"),
                                                Row.of("Ivana", "Research"))
                                        .build())
                        .runSql(
                                "insert into MySink "
                                        + "SELECT name, department_name FROM EMPLOYEE, DEPARTMENT where salary = b2 and salary < CAST(2 AS BIGINT)")
                        .build();

        INNER_JOIN_WITH_DUPLICATE_KEY =
                TableTestProgram.of(
                                "join-inner-join-with-duplicate-key",
                                "inner join with duplicate key")
                        .setupTableSource(EMPLOYEE)
                        .setupTableSource(DEPARTMENT)
                        .setupTableSink(
                                SinkTestStep.newBuilder("MySink")
                                        .addSchema("deptno int", "department_num int")
                                        .consumedBeforeRestore(Row.of(2, 2))
                                        .consumedAfterRestore(Row.of(4, 4), Row.of(4, 4))
                                        .build())
                        .runSql(
                                "insert into MySink "
                                        + "SELECT deptno, department_num FROM EMPLOYEE JOIN DEPARTMENT ON deptno = department_num AND deptno = b3")
                        .build();

        INNER_JOIN_WITH_NON_EQUI_JOIN =
                TableTestProgram.of(
                                "join-inner-join-with-non-equi-join",
                                "inner join with non-equi join")
                        .setupTableSource(EMPLOYEE)
                        .setupTableSource(DEPARTMENT)
                        .setupTableSink(
                                SinkTestStep.newBuilder("MySink")
                                        .addSchema("name varchar", "department_name varchar")
                                        .consumedBeforeRestore(Row.of("Don", "Sales"))
                                        .consumedAfterRestore(
                                                Row.of("Helena", "Engineering"),
                                                Row.of("Juliet", "Engineering"))
                                        .build())
                        .runSql(
                                "insert into MySink "
                                        + "SELECT name, department_name FROM EMPLOYEE JOIN DEPARTMENT ON deptno = department_num AND salary > b2")
                        .build();

        String query1 = "SELECT MIN(salary) AS salary, deptno FROM EMPLOYEE GROUP BY deptno";
        String query2 =
                "SELECT MIN(b2) AS b2, department_num FROM DEPARTMENT GROUP BY department_num";

        INNER_JOIN_WITH_EQUAL_PK =
                TableTestProgram.of("join-inner-join-with-equal-pk", "inner join with equal pk")
                        .setupTableSource(EMPLOYEE)
                        .setupTableSource(DEPARTMENT)
                        .setupTableSink(
                                SinkTestStep.newBuilder("MySink")
                                        .addSchema("deptno int", "department_num int")
                                        .consumedBeforeRestore(
                                                Row.of(1, 1), Row.of(2, 2), Row.of(3, 3))
                                        .consumedAfterRestore(Row.of(4, 4))
                                        .build())
                        .runSql(
                                String.format(
                                        "INSERT INTO MySink SELECT deptno, department_num FROM (%s) JOIN (%s) ON deptno = department_num",
                                        query1, query2))
                        .build();

        INNER_JOIN_WITH_PK =
                TableTestProgram.of("join-inner-join-with-pk", "inner join with pk")
                        .setupTableSource(EMPLOYEE)
                        .setupTableSource(DEPARTMENT)
                        .setupTableSink(
                                SinkTestStep.newBuilder("MySink")
                                        .addSchema("deptno int", "department_num int")
                                        .consumedBeforeRestore(
                                                Row.of(1, 1),
                                                Row.of(2, 2),
                                                Row.of(3, 2),
                                                Row.of(1, 3))
                                        .consumedAfterRestore(Row.of(3, 4), Row.of(2, 4))
                                        .testMaterializedData()
                                        .build())
                        .runSql(
                                String.format(
                                        "INSERT INTO MySink SELECT deptno, department_num FROM (%s) JOIN (%s) ON salary = b2",
                                        query1, query2))
                        .build();

        FULL_OUTER =
                TableTestProgram.of("join-outer-join", "outer join")
                        .setupTableSource(EMPLOYEE)
                        .setupTableSource(DEPARTMENT)
                        .setupTableSink(
                                SinkTestStep.newBuilder("MySink")
                                        .addSchema("name varchar", "department_name varchar")
                                        .consumedBeforeRestore(
                                                Row.of("Adam", null),
                                                Row.of(null, "Accounting"),
                                                Row.of("Baker", "Research"),
                                                Row.of("Charlie", "Human Resources"),
                                                Row.of("Charlie", "HR"),
                                                Row.of("Don", "Sales"),
                                                Row.of("Victor", null))
                                        .consumedAfterRestore(
                                                Row.of("Helena", "Engineering"),
                                                Row.of("Juliet", "Engineering"),
                                                Row.of("Ivana", "Research"),
                                                Row.of("Charlie", "People Operations"))
                                        .testMaterializedData()
                                        .build())
                        .runSql(
                                "insert into MySink "
                                        + "SELECT name, department_name FROM EMPLOYEE FULL OUTER JOIN DEPARTMENT ON deptno = department_num")
                        .build();

        LEFT_JOIN =
                TableTestProgram.of("join-left-join", "left join")
                        .setupTableSource(EMPLOYEE)
                        .setupTableSource(DEPARTMENT)
                        .setupTableSink(
                                SinkTestStep.newBuilder("MySink")
                                        .addSchema("name varchar", "department_name varchar")
                                        .consumedBeforeRestore(
                                                Row.of("Adam", null),
                                                Row.of("Baker", "Research"),
                                                Row.of("Charlie", "Human Resources"),
                                                Row.of("Charlie", "HR"),
                                                Row.of("Don", "Sales"),
                                                Row.of("Victor", null))
                                        .consumedAfterRestore(
                                                Row.of("Helena", "Engineering"),
                                                Row.of("Juliet", "Engineering"),
                                                Row.of("Ivana", "Research"),
                                                Row.of("Charlie", "People Operations"))
                                        .testMaterializedData()
                                        .build())
                        .runSql(
                                "insert into MySink "
                                        + "SELECT name, department_name FROM EMPLOYEE LEFT JOIN DEPARTMENT ON deptno = department_num")
                        .build();

        RIGHT_JOIN =
                TableTestProgram.of("join-right-join", "right join")
                        .setupTableSource(EMPLOYEE)
                        .setupTableSource(DEPARTMENT)
                        .setupTableSink(
                                SinkTestStep.newBuilder("MySink")
                                        .addSchema("name varchar", "department_name varchar")
                                        .consumedBeforeRestore(
                                                Row.of(null, "Accounting"),
                                                Row.of("Baker", "Research"),
                                                Row.of("Charlie", "Human Resources"),
                                                Row.of("Charlie", "HR"),
                                                Row.of("Don", "Sales"))
                                        .consumedAfterRestore(
                                                Row.of("Helena", "Engineering"),
                                                Row.of("Juliet", "Engineering"),
                                                Row.of("Ivana", "Research"),
                                                Row.of("Charlie", "People Operations"))
                                        .testMaterializedData()
                                        .build())
                        .runSql(
                                "insert into MySink "
                                        + "SELECT name, department_name FROM EMPLOYEE RIGHT OUTER JOIN DEPARTMENT ON deptno = department_num")
                        .build();

        SEMI_JOIN =
                TableTestProgram.of("join-semi-join", "semi join")
                        .setupTableSource(EMPLOYEE)
                        .setupTableSource(DEPARTMENT)
                        .setupTableSink(
                                SinkTestStep.newBuilder("MySink")
                                        .addSchema("name varchar")
                                        .consumedBeforeRestore(
                                                Row.of("Baker"), Row.of("Charlie"), Row.of("Don"))
                                        .consumedAfterRestore(
                                                Row.of("Helena"), Row.of("Juliet"), Row.of("Ivana"))
                                        .build())
                        .runSql(
                                "insert into MySink "
                                        + "SELECT name FROM EMPLOYEE WHERE deptno IN (SELECT department_num FROM DEPARTMENT)")
                        .build();

        ANTI_JOIN =
                TableTestProgram.of("join-anti-join", "anti join")
                        .setupTableSource(EMPLOYEE)
                        .setupTableSource(DEPARTMENT_NONULLS)
                        .setupTableSink(
                                SinkTestStep.newBuilder("MySink")
                                        .addSchema("name varchar")
                                        .consumedBeforeRestore(Row.of("Victor"))
                                        .consumedAfterRestore(Row.of("Juliet"), Row.of("Helena"))
                                        .testMaterializedData()
                                        .build())
                        .runSql(
                                "insert into MySink "
                                        + "SELECT name FROM EMPLOYEE WHERE deptno NOT IN (SELECT department_num FROM DEPARTMENT)")
                        .build();

        JOIN_WITH_STATE_TTL_HINT =
                TableTestProgram.of("join-with-state-ttl-hint", "join with state ttl hint")
                        .setupTableSource(EMPLOYEE)
                        .setupTableSource(DEPARTMENT)
                        .setupTableSink(
                                SinkTestStep.newBuilder("MySink")
                                        .addSchema("deptno int", "department_num int")
                                        .consumedBeforeRestore(
                                                Row.of(1, 1), Row.of(2, 2), Row.of(3, 3))
                                        .consumedAfterRestore(Row.of(4, 4))
                                        .build())
                        .runSql(
                                String.format(
                                        "INSERT INTO MySink SELECT /*+ STATE_TTL('v1' = '1d', 'v2' = '4d'), STATE_TTL('v2' = '8d') */deptno, department_num FROM (%s) v1 JOIN (%s) v2 ON deptno = department_num",
                                        query1, query2))
                        .build();
    }
}
