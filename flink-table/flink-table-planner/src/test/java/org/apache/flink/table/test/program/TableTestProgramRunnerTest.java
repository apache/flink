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

package org.apache.flink.table.test.program;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.test.program.TestStep.TestKind;
import org.apache.flink.table.utils.UserDefinedFunctions;

import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TableTestProgram} and {@link TableTestProgramRunner}. */
public class TableTestProgramRunnerTest {

    private static final String ID = "id";
    private static final String DESCRIPTION = "description";

    @Test
    void testConfigStep() {
        final TableTestProgram program =
                TableTestProgram.of(ID, DESCRIPTION)
                        .setupConfig(TableConfigOptions.LOCAL_TIME_ZONE, "GMT+3")
                        .build();

        assertThat(program.setupSteps).hasSize(1);

        final TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        program.getSetupConfigOptionTestSteps().forEach(s -> s.apply(env));
        assertThat(env.getConfig().getLocalTimeZone()).isEqualTo(ZoneId.of("GMT+3"));
    }

    @Test
    void testFunctionStep() {
        final TableTestProgram program =
                TableTestProgram.of(ID, DESCRIPTION)
                        .setupTemporarySystemFunction(
                                "tmp_sys", UserDefinedFunctions.ScalarUDF.class)
                        .setupTemporaryCatalogFunction(
                                "tmp_cat", UserDefinedFunctions.ScalarUDF.class)
                        .setupCatalogFunction("cat", UserDefinedFunctions.ScalarUDF.class)
                        .build();

        assertThat(program.setupSteps).hasSize(3);

        final TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        program.getSetupFunctionTestSteps().forEach(s -> s.apply(env));

        assertThat(env.listUserDefinedFunctions()).contains("tmp_sys", "tmp_cat", "cat");
    }

    @Test
    void testSqlStep() {
        final TableTestProgram program =
                TableTestProgram.of(ID, DESCRIPTION)
                        .setupSql("CREATE TABLE MyTable1 (i INT) WITH ('connector' = 'datagen')")
                        .runSql("CREATE TABLE MyTable2 (i INT) WITH ('connector' = 'datagen')")
                        .build();

        assertThat(program.setupSteps).hasSize(1);
        assertThat(program.runSteps).hasSize(1);

        final TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        program.setupSteps.stream().map(SqlTestStep.class::cast).forEach(s -> s.apply(env));
        program.runSteps.stream().map(SqlTestStep.class::cast).forEach(s -> s.apply(env));

        assertThat(env.listTables()).contains("MyTable1", "MyTable2");
    }

    @Test
    @SuppressWarnings("resource")
    void testTableStep() {
        final TableTestProgram program =
                TableTestProgram.of(ID, DESCRIPTION)
                        .setupTableSource(
                                SourceTestStep.newBuilder("MyTableSource")
                                        .addSchema("i INT")
                                        .addOption("connector", "datagen")
                                        .build())
                        .setupTableSink(
                                SinkTestStep.newBuilder("MyTableSink")
                                        .addSchema("i INT")
                                        .addOption("connector", "blackhole")
                                        .build())
                        .build();

        assertThat(program.setupSteps).hasSize(2);

        final TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        program.getSetupSourceTestSteps()
                .forEach(s -> s.apply(env, Collections.singletonMap("number-of-rows", "3")));
        program.getSetupSinkTestSteps().forEach(s -> s.apply(env));

        assertThat(env.executeSql("SHOW CREATE TABLE MyTableSource").collect().next().getField(0))
                .isEqualTo(
                        "CREATE TABLE `default_catalog`.`default_database`.`MyTableSource` (\n"
                                + "  `i` INT\n"
                                + ") WITH (\n"
                                + "  'connector' = 'datagen',\n"
                                + "  'number-of-rows' = '3'\n"
                                + ")\n");
        assertThat(env.executeSql("SHOW CREATE TABLE MyTableSink").collect().next().getField(0))
                .isEqualTo(
                        "CREATE TABLE `default_catalog`.`default_database`.`MyTableSink` (\n"
                                + "  `i` INT\n"
                                + ") WITH (\n"
                                + "  'connector' = 'blackhole'\n"
                                + ")\n");
    }

    @Test
    void testRunnerValidationDuplicate() {
        final TableTestProgram program1 =
                TableTestProgram.of(ID, DESCRIPTION).runSql("SELECT 1").build();

        final TableTestProgram program2 =
                TableTestProgram.of(ID, DESCRIPTION).runSql("SELECT 1").build();

        final LimitedTableTestProgramRunner runner = new LimitedTableTestProgramRunner();
        runner.programs = Arrays.asList(program1, program2);

        assertThatThrownBy(runner::supportedPrograms)
                .hasMessageContaining("Duplicate test program id found: [id]");
    }

    @Test
    void testRunnerValidationUnsupported() {
        final LimitedTableTestProgramRunner runner = new LimitedTableTestProgramRunner();

        final TableTestProgram program =
                TableTestProgram.of(ID, DESCRIPTION).setupSql("SELECT 1").build();

        runner.programs = Collections.singletonList(program);

        assertThatThrownBy(runner::supportedPrograms)
                .hasMessageContaining("Test runner does not support setup step: SQL");
    }

    private static class LimitedTableTestProgramRunner implements TableTestProgramRunner {

        List<TableTestProgram> programs;

        @Override
        public List<TableTestProgram> programs() {
            return programs;
        }

        @Override
        public EnumSet<TestKind> supportedSetupSteps() {
            return EnumSet.of(TestKind.SOURCE_WITH_DATA);
        }

        @Override
        public EnumSet<TestKind> supportedRunSteps() {
            return EnumSet.of(TestKind.SQL);
        }
    }
}
