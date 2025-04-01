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

package org.apache.flink.table.planner.runtime.common.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** IT case for merging table sink. */
@ExtendWith(ParameterizedTestExtension.class)
public class SinkReuseITCase extends AbstractTestBase {
    @Parameter public Boolean isBatch;

    @Parameters(name = "isBatch: {0}")
    public static Collection<Boolean> parameters() {
        return List.of(true, false);
    }

    TableEnvironment tEnv;

    void setup(boolean isBatch) throws Exception {
        EnvironmentSettings settings;
        if (isBatch) {
            settings = EnvironmentSettings.newInstance().inBatchMode().build();
            tEnv = TableEnvironmentImpl.create(settings);
        } else {
            settings = EnvironmentSettings.newInstance().inStreamingMode().build();
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(4);
            env.getConfig().enableObjectReuse();
            tEnv = StreamTableEnvironment.create(env, settings);
        }

        tEnv.getConfig()
                .getConfiguration()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SINK_ENABLED, true);

        String dataId1 =
                TestValuesTableFactory.registerData(
                        Arrays.asList(Row.of(1, 1.1d, "Tom"), Row.of(2, 1.2d, "Jerry")));

        String dataId2 =
                TestValuesTableFactory.registerData(
                        Arrays.asList(Row.of(1, 2.1d, "Alice"), Row.of(2, 2.2d, "Bob")));

        String dataId3 =
                TestValuesTableFactory.registerData(
                        Arrays.asList(Row.of(1, 3.1d, "Jack"), Row.of(2, 3.2d, "Rose")));

        createSourceTable("source1", getSourceOptions(dataId1));
        createSourceTable("source2", getSourceOptions(dataId2));
        createSourceTable("source3", getSourceOptions(dataId3));

        createSinkTable("sink1", getSinkOptions());
        createSinkTable("sink2", getSinkOptions());
    }

    @TestTemplate
    public void testSinkMergeFromSameSource() throws Exception {
        setup(isBatch);
        StatementSet statementSet = tEnv.createStatementSet();
        statementSet.addInsertSql("INSERT INTO sink1 SELECT * FROM source1");
        statementSet.addInsertSql("INSERT INTO sink1 SELECT * FROM source1");
        statementSet.execute().await();

        List<String> sink1Result = TestValuesTableFactory.getResultsAsStrings("sink1");
        List<String> sink1Expected =
                Arrays.asList(
                        "+I[1, 1.1, Tom]",
                        "+I[2, 1.2, Jerry]",
                        "+I[1, 1.1, Tom]",
                        "+I[2, 1.2, Jerry]");
        assertResult(sink1Expected, sink1Result);
    }

    @TestTemplate
    public void testMergeSink() throws Exception {
        setup(isBatch);
        StatementSet statementSet = tEnv.createStatementSet();
        statementSet.addInsertSql("INSERT INTO sink1 SELECT * FROM source1");
        statementSet.addInsertSql("INSERT INTO sink1 SELECT * FROM source2");
        statementSet.addInsertSql("INSERT INTO sink2 SELECT * FROM source3");
        statementSet.execute().await();

        List<String> sink1Result = TestValuesTableFactory.getResultsAsStrings("sink1");
        List<String> sink2Result = TestValuesTableFactory.getResultsAsStrings("sink2");

        List<String> sink1Expected =
                Arrays.asList(
                        "+I[1, 1.1, Tom]",
                        "+I[2, 1.2, Jerry]",
                        "+I[1, 2.1, Alice]",
                        "+I[2, 2.2, Bob]");
        List<String> sink3Expected = Arrays.asList("+I[1, 3.1, Jack]", "+I[2, 3.2, Rose]");

        assertResult(sink1Expected, sink1Result);
        assertResult(sink3Expected, sink2Result);
    }

    private Map<String, String> getSourceOptions(String dataId) {
        Map<String, String> sourceOptions = new HashMap<>();
        sourceOptions.put("connector", "values");
        sourceOptions.put("bounded", "true");
        sourceOptions.put("data-id", dataId);
        return sourceOptions;
    }

    private Map<String, String> getSinkOptions() {
        Map<String, String> sinkOptions = new HashMap<>();
        sinkOptions.put("connector", "values");
        sinkOptions.put("bounded", "true");
        return sinkOptions;
    }

    private void createSinkTable(String tableName, Map<String, String> options) {
        String ddl =
                String.format(
                        "CREATE TABLE `%s` (\n"
                                + "  a int,\n"
                                + "  b double,\n"
                                + "  c string\n"
                                + ")  WITH (\n"
                                + " %s \n"
                                + ")",
                        tableName, makeWithOptions(options));
        tEnv.executeSql(ddl);
    }

    private void createSourceTable(String tableName, Map<String, String> options) {
        String ddl =
                String.format(
                        "CREATE TABLE `%s` (\n"
                                + "  a int,\n"
                                + "  b double,\n"
                                + "  c string,\n"
                                + "  PRIMARY KEY (a) NOT ENFORCED\n"
                                + ")  WITH (\n"
                                + " %s \n"
                                + ")",
                        tableName, makeWithOptions(options));
        tEnv.executeSql(ddl);
    }

    private String makeWithOptions(Map<String, String> options) {
        return options.keySet().stream()
                .map(key -> String.format("  '%s' = '%s'", key, options.get(key)))
                .collect(Collectors.joining(",\n"));
    }

    private void assertResult(List<String> expected, List<String> actual) {
        Collections.sort(expected);
        Collections.sort(actual);
        assertThat(actual).isEqualTo(expected);
    }
}
