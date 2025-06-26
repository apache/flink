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
import org.apache.flink.table.planner.functions.sql.ml.MLEvaluationAggregationFunction;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link MLEvaluationAggregationFunction}. */
public class MLEvaluationAggregationFunctionTest {

    @Test
    public void testInvalidTaskInConstructor() {
        assertThatThrownBy(() -> MLEvaluationAggregationFunction.create("invalid_task"))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "Invalid task type: 'invalid_task'. Supported task types are: [classification, clustering, embedding, regression, text_generation].");
    }

    @Test
    public void testClusteringTaskInConstructor() {
        assertThatThrownBy(() -> MLEvaluationAggregationFunction.create("clustering"))
                .isInstanceOf(ValidationException.class)
                .hasMessage("Task 'clustering' is not supported for evaluation.");
    }
}
