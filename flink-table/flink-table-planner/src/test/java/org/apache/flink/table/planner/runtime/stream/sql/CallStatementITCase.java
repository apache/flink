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

package org.apache.flink.table.planner.runtime.stream.sql;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.ProcedureNotExistException;
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.table.procedures.Procedure;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for procedures in a table environment. */
public class CallStatementITCase extends StreamingTestBase {

    @BeforeEach
    @Override
    public void before() throws Exception {
        super.before();
        CatalogWithBuildInProcedure procedureCatalog =
                new CatalogWithBuildInProcedure("procedure_catalog");
        tEnv().registerCatalog("test_p", procedureCatalog);
        tEnv().useCatalog("test_p");
    }

    @Test
    public void testCallStatement() {
        // test call procedure can run a flink job
        TableResult tableResult = tEnv().executeSql("call `system`.generate_n(4)");
        verifyTableResult(
                tableResult,
                Arrays.asList(Row.of(0), Row.of(1), Row.of(2), Row.of(3)),
                ResolvedSchema.of(
                        Column.physical(
                                "result", DataTypes.BIGINT().notNull().bridgedTo(long.class))));

        // call a procedure which will run in batch mode
        tableResult = tEnv().executeSql("call `system`.generate_n(4, 'BATCH')");
        verifyTableResult(
                tableResult,
                Arrays.asList(Row.of(0), Row.of(1), Row.of(2), Row.of(3)),
                ResolvedSchema.of(
                        Column.physical(
                                "result", DataTypes.BIGINT().notNull().bridgedTo(long.class))));
        // check the runtime mode in current env is still streaming
        assertThat(tEnv().getConfig().get(ExecutionOptions.RUNTIME_MODE))
                .isEqualTo(RuntimeExecutionMode.STREAMING);

        // test call procedure with var-args as well as output data type hint
        tableResult = tEnv().executeSql("call `system`.sum_n(5.5, 1.2, 3.3)");
        verifyTableResult(
                tableResult,
                Collections.singletonList(Row.of("10.00", 3)),
                ResolvedSchema.of(
                        Column.physical("sum_value", DataTypes.DECIMAL(10, 2)),
                        Column.physical("count", DataTypes.INT())));

        // test call procedure with timestamp as input
        tableResult =
                tEnv().executeSql(
                                "call `system`.get_year(timestamp '2023-04-22 00:00:00', timestamp '2024-04-22 00:00:00.300')");
        verifyTableResult(
                tableResult,
                Arrays.asList(Row.of(2023), Row.of(2024)),
                ResolvedSchema.of(Column.physical("result", DataTypes.STRING())));

        // test call procedure with pojo as return type
        tableResult = tEnv().executeSql("call `system`.generate_user('yuxia', 18)");
        verifyTableResult(
                tableResult,
                Collections.singletonList(Row.of(new UserPojo("yuxia", 18))),
                ResolvedSchema.of(
                        Column.physical("name", DataTypes.STRING()),
                        Column.physical("age", DataTypes.INT().notNull().bridgedTo(int.class))));
    }

    private void verifyTableResult(
            TableResult tableResult, List<Row> expectedResult, ResolvedSchema expectedSchema) {
        assertThat(CollectionUtil.iteratorToList(tableResult.collect()).toString())
                .isEqualTo(expectedResult.toString());
        assertThat(tableResult.getResolvedSchema()).isEqualTo(expectedSchema);
    }

    /** A catalog with some built-in procedures for test purpose. */
    private static class CatalogWithBuildInProcedure extends GenericInMemoryCatalog {

        private static final Map<ObjectPath, Procedure> PROCEDURE_MAP = new HashMap<>();

        static {
            PROCEDURE_MAP.put(
                    ObjectPath.fromString("system.generate_n"), new GenerateSequenceProcedure());
            PROCEDURE_MAP.put(ObjectPath.fromString("system.sum_n"), new SumProcedure());
            PROCEDURE_MAP.put(ObjectPath.fromString("system.get_year"), new GetYearProcedure());
            PROCEDURE_MAP.put(
                    ObjectPath.fromString("system.generate_user"), new GenerateUserProcedure());
            PROCEDURE_MAP.put(
                    ObjectPath.fromString("system.miss_procedure_context"),
                    new MissProcedureContextProcedure());
        }

        public CatalogWithBuildInProcedure(String name) {
            super(name);
        }

        @Override
        public Procedure getProcedure(ObjectPath procedurePath)
                throws ProcedureNotExistException, CatalogException {
            if (PROCEDURE_MAP.containsKey(procedurePath)) {
                return PROCEDURE_MAP.get(procedurePath);
            } else {
                throw new ProcedureNotExistException(getName(), procedurePath);
            }
        }
    }

    public static class GenerateSequenceProcedure implements Procedure {
        public long[] call(ProcedureContext procedureContext, int n) throws Exception {
            return generate(procedureContext.getExecutionEnvironment(), n);
        }

        public long[] call(ProcedureContext procedureContext, int n, String runTimeMode)
                throws Exception {
            StreamExecutionEnvironment env = procedureContext.getExecutionEnvironment();
            env.setRuntimeMode(RuntimeExecutionMode.valueOf(runTimeMode));
            return generate(env, n);
        }

        private long[] generate(StreamExecutionEnvironment env, int n) throws Exception {
            env.setParallelism(1);
            long[] sequenceN = new long[n];
            int i = 0;
            try (CloseableIterator<Long> result = env.fromSequence(0, n - 1).executeAndCollect()) {
                while (result.hasNext()) {
                    sequenceN[i++] = result.next();
                }
            }
            return sequenceN;
        }
    }

    public static class SumProcedure implements Procedure {

        public @DataTypeHint("ROW< sum_value decimal(10, 2), count INT >") Row[] call(
                ProcedureContext procedureContext,
                @DataTypeHint("DECIMAL(10, 2)") BigDecimal... inputs) {
            if (inputs.length == 0) {
                return new Row[] {Row.of(null, 0)};
            }
            int counts = inputs.length;
            BigDecimal result = inputs[0];
            for (int i = 1; i < inputs.length; i++) {
                result = result.add(inputs[i]);
            }
            return new Row[] {Row.of(result, counts)};
        }
    }

    public static class GetYearProcedure implements Procedure {
        public String[] call(ProcedureContext procedureContext, LocalDateTime... timestamps) {
            String[] results = new String[timestamps.length];
            for (int i = 0; i < results.length; i++) {
                results[i] = String.valueOf(timestamps[i].getYear());
            }
            return results;
        }
    }

    public static class GenerateUserProcedure implements Procedure {
        public UserPojo[] call(ProcedureContext procedureContext, String name, Integer age) {
            return new UserPojo[] {new UserPojo(name, age)};
        }
    }

    public static class MissProcedureContextProcedure implements Procedure {
        public int[] call(ProcedureContext context, int a) {
            return null;
        }
    }

    public static class UserPojo {
        private final String name;
        private final int age;

        public UserPojo(String name, int age) {
            this.name = name;
            this.age = age;
        }

        public String getName() {
            return name;
        }

        public int getAge() {
            return age;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            UserPojo userPojo = (UserPojo) o;
            return age == userPojo.age && Objects.equals(name, userPojo.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, age);
        }

        @Override
        public String toString() {
            return "UserPojo{" + "name='" + name + '\'' + ", age=" + age + '}';
        }
    }
}
