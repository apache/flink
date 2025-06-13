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

package org.apache.flink.table.planner.functions.sql.ml.evaluate;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.ml.TaskType;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link MLEvaluationAggregationFunction}. */
public class MLEvaluationAggregationFunctionTest {

    @Test
    public void testTaskMapConsistency() {
        List<TaskType> evaluationTasks =
                Stream.of(TaskType.values())
                        .filter(TaskType::supportsEvaluation)
                        .collect(Collectors.toList());
        assertThat(MLEvaluationAggregationFunction.TASK_TYPE_MAP.size())
                .isEqualTo(evaluationTasks.size());
        for (TaskType taskType : evaluationTasks) {
            assertThat(MLEvaluationAggregationFunction.TASK_TYPE_MAP).containsKey(taskType);
        }
    }

    @Test
    public void testInvalidTaskInConstructor() {
        assertThatThrownBy(() -> new MLEvaluationAggregationFunction("invalid_task"))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "Invalid task type: 'invalid_task'. Supported task types are: [classification, clustering, embedding, regression, text_generation].");
    }

    @Test
    public void testClusteringTaskInConstructor() {
        assertThatThrownBy(() -> new MLEvaluationAggregationFunction("clustering"))
                .isInstanceOf(ValidationException.class)
                .hasMessage("Task clustering is not supported for evaluation.");
    }

    public static Object getMapValue(Row result, String key) {
        return ((Map<?, ?>) result.getField(0)).get(key);
    }
}
