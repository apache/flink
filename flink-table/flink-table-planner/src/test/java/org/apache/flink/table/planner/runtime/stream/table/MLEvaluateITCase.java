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

package org.apache.flink.table.planner.runtime.stream.table;

import org.apache.flink.table.planner.factories.TestValuesModelFactory;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

/** ITCase for ML evaluation with different task types. */
public class MLEvaluateITCase extends StreamingTestBase {

    private final List<Row> classificationData =
            Arrays.asList(
                    Row.of("positive", "positive"),
                    Row.of("negative", "negative"),
                    Row.of("negative", "positive"),
                    Row.of("positive", "negative"));

    private final List<Row> regressionData =
            Arrays.asList(
                    Row.of(1.0, 1.0),
                    Row.of(2.0, 2.0),
                    Row.of(3.0, 3.0),
                    Row.of(null, 4.0),
                    Row.of(5.0, 5.0));

    private final List<Row> embeddingData =
            Arrays.asList(
                    Row.of(new Float[] {1.0f, 2.0f}, "text0"),
                    Row.of(new Float[] {2.0f, 3.0f}, "text1"),
                    Row.of(new Float[] {3.0f, 4.0f}, "text2"));

    private final List<Row> textGenerationData =
            Arrays.asList(
                    Row.of("result0", "input0"),
                    Row.of("result1", "input1"),
                    Row.of("result2", "input2"));

    @BeforeEach
    public void before() throws Exception {
        super.before();
        createClassificationTable();
        createRegressionTable();
        createEmbeddingTable();
        createTextGenerationTable();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testClassificationEvaluation(boolean async) {
        List<Row> result =
                CollectionUtil.iteratorToList(
                        tEnv().executeSql(
                                        "SELECT * FROM ML_EVALUATE("
                                                + "TABLE classification_table, "
                                                + "MODEL classification_model, "
                                                + "DESCRIPTOR(label), "
                                                + "DESCRIPTOR(text), 'classification', "
                                                + "MAP['async', '"
                                                + async
                                                + "'])")
                                .collect());

        List<String> expected =
                Arrays.asList(
                        "+I[{Accuracy=1.0, Precision=1.0, Recall=1.0, F1=1.0}]",
                        "-U[{Accuracy=1.0, Precision=1.0, Recall=1.0, F1=1.0}]",
                        "+U[{Accuracy=1.0, Precision=1.0, Recall=1.0, F1=1.0}]",
                        "-U[{Accuracy=1.0, Precision=1.0, Recall=1.0, F1=1.0}]",
                        "+U[{Accuracy=0.6666666666666666, Precision=0.75, Recall=0.75, F1=0.75}]",
                        "-U[{Accuracy=0.6666666666666666, Precision=0.75, Recall=0.75, F1=0.75}]",
                        "+U[{Accuracy=0.5, Precision=0.5, Recall=0.5, F1=0.5}]");
        List<String> actual = result.stream().map(Objects::toString).collect(Collectors.toList());
        assertThat(actual).isEqualTo(expected);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testRegressionEvaluation(boolean async) {
        List<Row> result =
                CollectionUtil.iteratorToList(
                        tEnv().executeSql(
                                        "SELECT * FROM ML_EVALUATE("
                                                + "TABLE regression_table, "
                                                + "MODEL regression_model, "
                                                + "DESCRIPTOR(label), "
                                                + "DESCRIPTOR(feature), "
                                                + "'regression', MAP['async', '"
                                                + async
                                                + "'])")
                                .collect());

        List<String> expected =
                Arrays.asList(
                        "+I[{MAE=0.0, R2=NaN, MAPE=0.0, RMSE=0.0, MSE=0.0}]",
                        "-U[{MAE=0.0, R2=NaN, MAPE=0.0, RMSE=0.0, MSE=0.0}]",
                        "+U[{MAE=0.5, R2=-1.0, MAPE=25.0, RMSE=0.7071067811865476, MSE=0.5}]",
                        "-U[{MAE=0.5, R2=-1.0, MAPE=25.0, RMSE=0.7071067811865476, MSE=0.5}]",
                        "+U[{MAE=0.3333333333333333, R2=0.5, MAPE=16.666666666666668, RMSE=0.5773502691896257, MSE=0.3333333333333333}]",
                        "-U[{MAE=0.3333333333333333, R2=0.5, MAPE=16.666666666666668, RMSE=0.5773502691896257, MSE=0.3333333333333333}]",
                        "+U[{MAE=0.3333333333333333, R2=0.5, MAPE=16.666666666666668, RMSE=0.5773502691896257, MSE=0.3333333333333333}]",
                        "-U[{MAE=0.3333333333333333, R2=0.5, MAPE=16.666666666666668, RMSE=0.5773502691896257, MSE=0.3333333333333333}]",
                        "+U[{MAE=0.3333333333333333, R2=0.5, MAPE=16.666666666666668, RMSE=0.5773502691896257, MSE=0.3333333333333333}]");
        List<String> actual = result.stream().map(Objects::toString).collect(Collectors.toList());
        assertThat(actual).isEqualTo(expected);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testEmbeddingEvaluation(boolean async) {
        List<Row> result =
                CollectionUtil.iteratorToList(
                        tEnv().executeSql(
                                        "SELECT * FROM ML_EVALUATE("
                                                + "TABLE embedding_table, "
                                                + "MODEL embedding_model, "
                                                + "DESCRIPTOR(label), "
                                                + "DESCRIPTOR(text), "
                                                + "'embedding', MAP['async', '"
                                                + async
                                                + "'])")
                                .collect());

        List<String> expected =
                Arrays.asList(
                        "+I[{MJS=1.0, MCS=1.0, MED=0.0}]",
                        "-U[{MJS=1.0, MCS=1.0, MED=0.0}]",
                        "+U[{MJS=1.0, MCS=1.0, MED=0.0}]",
                        "-U[{MJS=1.0, MCS=1.0, MED=0.0}]",
                        "+U[{MJS=0.9259259259259259, MCS=0.999837358695693, MED=0.47140452079103173}]");
        List<String> actual = result.stream().map(Objects::toString).collect(Collectors.toList());
        assertThat(actual).isEqualTo(expected);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testTextGenerationEvaluation(boolean async) {
        List<Row> result =
                CollectionUtil.iteratorToList(
                        tEnv().executeSql(
                                        "SELECT * FROM ML_EVALUATE("
                                                + "TABLE text_generation_table, "
                                                + "MODEL text_generation_model, "
                                                + "DESCRIPTOR(label), "
                                                + "DESCRIPTOR(text), "
                                                + "'text_generation', MAP['async', '"
                                                + async
                                                + "'])")
                                .collect());

        List<String> expected =
                Arrays.asList(
                        "+I[{MB=1.0, MR=1.0, MSS=1.0}]",
                        "-U[{MB=1.0, MR=1.0, MSS=1.0}]",
                        "+U[{MB=1.0, MR=1.0, MSS=1.0}]",
                        "-U[{MB=1.0, MR=1.0, MSS=1.0}]",
                        "+U[{MB=0.6666666666666666, MR=0.6666666666666666, MSS=0.6666666666666666}]");

        List<String> actual = result.stream().map(Objects::toString).collect(Collectors.toList());
        assertThat(actual).isEqualTo(expected);
    }

    private void createClassificationTable() {
        String dataId = TestValuesTableFactory.registerData(classificationData);
        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE classification_table(\n"
                                        + "  label STRING,\n"
                                        + "  text STRING\n"
                                        + ") WITH (\n"
                                        + "  'connector' = 'values',\n"
                                        + "  'data-id' = '%s'\n"
                                        + ")",
                                dataId));

        Map<Row, List<Row>> modelData = new HashMap<>();
        modelData.put(Row.of("positive"), Collections.singletonList(Row.of("positive")));
        modelData.put(Row.of("negative"), Collections.singletonList(Row.of("negative")));

        tEnv().executeSql(
                        String.format(
                                "CREATE MODEL classification_model\n"
                                        + "INPUT (text STRING)\n"
                                        + "OUTPUT (prediction STRING)\n"
                                        + "WITH (\n"
                                        + "  'provider' = 'values',"
                                        + "  'data-id' = '%s',"
                                        + "  'async' = 'true',"
                                        + "  'task' = 'classification'"
                                        + ")",
                                TestValuesModelFactory.registerData(modelData)));
    }

    private void createRegressionTable() {
        String dataId = TestValuesTableFactory.registerData(regressionData);
        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE regression_table(\n"
                                        + "  label DOUBLE,\n"
                                        + "  feature DOUBLE\n"
                                        + ") WITH (\n"
                                        + "  'connector' = 'values',\n"
                                        + "  'data-id' = '%s'\n"
                                        + ")",
                                dataId));

        Map<Row, List<Row>> modelData = new HashMap<>();
        modelData.put(Row.of(1.0), Collections.singletonList(Row.of(1.0)));
        modelData.put(Row.of(2.0), Collections.singletonList(Row.of(3.0)));
        modelData.put(Row.of(3.0), Collections.singletonList(Row.of(3.0)));
        modelData.put(
                Row.of(4.0), Collections.singletonList(Row.of(4.0))); // Skipped since label is null
        Row nullRow = new Row(1);
        nullRow.setField(0, null);
        modelData.put(
                Row.of(5.0),
                Collections.singletonList(nullRow)); // Skipped since prediction is null

        tEnv().executeSql(
                        String.format(
                                "CREATE MODEL regression_model\n"
                                        + "INPUT (feature DOUBLE)\n"
                                        + "OUTPUT (prediction DOUBLE)\n"
                                        + "WITH (\n"
                                        + "  'provider' = 'values',"
                                        + "  'data-id' = '%s',"
                                        + "  'async' = 'true',"
                                        + "  'task' = 'regression'"
                                        + ")",
                                TestValuesModelFactory.registerData(modelData)));
    }

    private void createEmbeddingTable() {
        String dataId = TestValuesTableFactory.registerData(embeddingData);
        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE embedding_table(\n"
                                        + "  label ARRAY<FLOAT>,\n"
                                        + "  text STRING\n"
                                        + ") WITH (\n"
                                        + "  'connector' = 'values',\n"
                                        + "  'data-id' = '%s'\n"
                                        + ")",
                                dataId));

        Map<Row, List<Row>> modelData = new HashMap<>();
        modelData.put(
                Row.of("text0"),
                Collections.singletonList(Row.of((Object) new Float[] {1.0f, 2.0f})));
        modelData.put(
                Row.of("text1"),
                Collections.singletonList(Row.of((Object) new Float[] {2.0f, 3.0f})));
        modelData.put(
                Row.of("text2"),
                Collections.singletonList(Row.of((Object) new Float[] {4.0f, 5.0f})));

        tEnv().executeSql(
                        String.format(
                                "CREATE MODEL embedding_model\n"
                                        + "INPUT (text STRING)\n"
                                        + "OUTPUT (prediction ARRAY<FLOAT>)\n"
                                        + "WITH (\n"
                                        + "  'provider' = 'values',"
                                        + "  'data-id' = '%s',"
                                        + "  'async' = 'true',"
                                        + "  'task' = 'embedding'"
                                        + ")",
                                TestValuesModelFactory.registerData(modelData)));
    }

    private void createTextGenerationTable() {
        String dataId = TestValuesTableFactory.registerData(textGenerationData);
        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE text_generation_table(\n"
                                        + "  label STRING,\n"
                                        + "  text STRING\n"
                                        + ") WITH (\n"
                                        + "  'connector' = 'values',\n"
                                        + "  'data-id' = '%s'\n"
                                        + ")",
                                dataId));

        Map<Row, List<Row>> modelData = new HashMap<>();
        modelData.put(Row.of("input0"), Collections.singletonList(Row.of("result0")));
        modelData.put(Row.of("input1"), Collections.singletonList(Row.of("result1")));
        modelData.put(Row.of("input2"), Collections.singletonList(Row.of("different result")));

        tEnv().executeSql(
                        String.format(
                                "CREATE MODEL text_generation_model\n"
                                        + "INPUT (text STRING)\n"
                                        + "OUTPUT (prediction STRING)\n"
                                        + "WITH (\n"
                                        + "  'provider' = 'values',"
                                        + "  'data-id' = '%s',"
                                        + "  'async' = 'true',"
                                        + "  'task' = 'text_generation'"
                                        + ")",
                                TestValuesModelFactory.registerData(modelData)));
    }
}
