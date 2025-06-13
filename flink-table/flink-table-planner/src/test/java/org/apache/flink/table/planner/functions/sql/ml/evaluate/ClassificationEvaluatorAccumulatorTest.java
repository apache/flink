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

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.offset;

/** Test for ClassificationEvaluatorAccumulator. */
public class ClassificationEvaluatorAccumulatorTest {
    private static final double DELTA = 1e-6;

    @Test
    void testPerfectClassification() {
        ClassificationEvaluatorAccumulator accumulator = new ClassificationEvaluatorAccumulator();
        // Perfect predictions
        accumulator.accumulate("A", "A");
        accumulator.accumulate("B", "B");
        accumulator.accumulate("C", "C");
        Map<String, Double> result = accumulator.getValue();
        assertThat(result).isNotNull();
        assertThat(result.get("Accuracy")).isEqualTo(1.0, offset(DELTA));
        assertThat(result.get("Precision")).isEqualTo(1.0, offset(DELTA));
        assertThat(result.get("Recall")).isEqualTo(1.0, offset(DELTA));
        assertThat(result.get("F1")).isEqualTo(1.0, offset(DELTA));
    }

    @Test
    void testTypicalClassification() {
        ClassificationEvaluatorAccumulator accumulator = new ClassificationEvaluatorAccumulator();
        // Mix of correct and incorrect predictions
        accumulator.accumulate("A", "A"); // correct
        accumulator.accumulate("A", "B"); // incorrect
        accumulator.accumulate("B", "B"); // correct
        accumulator.accumulate("B", "A"); // incorrect
        Map<String, Double> result = accumulator.getValue();
        assertThat(result).isNotNull();
        assertThat(result.get("Accuracy")).isEqualTo(0.5, offset(DELTA));
        assertThat(result.get("Precision")).isEqualTo(0.5, offset(DELTA));
        assertThat(result.get("Recall")).isEqualTo(0.5, offset(DELTA));
        assertThat(result.get("F1")).isEqualTo(0.5, offset(DELTA));
    }

    @Test
    void testRetraction() {
        ClassificationEvaluatorAccumulator accumulator = new ClassificationEvaluatorAccumulator();
        // Add predictions
        accumulator.accumulate("A", "A");
        accumulator.accumulate("A", "B");

        Map<String, Double> result = accumulator.getValue();
        assertThat(result).isNotNull();
        assertThat(result.get("Accuracy")).isEqualTo(0.5, offset(DELTA));
        assertThat(result.get("Precision")).isEqualTo(1.0, offset(DELTA));
        assertThat(result.get("Recall")).isEqualTo(0.5, offset(DELTA));
        assertThat(result.get("F1")).isEqualTo(2.0 / 3.0, offset(DELTA));

        // Retract one prediction
        accumulator.retract("A", "B");
        result = accumulator.getValue();
        assertThat(result).isNotNull();
        assertThat(result.get("Accuracy")).isEqualTo(1.0, offset(DELTA));
        assertThat(result.get("Precision")).isEqualTo(1.0, offset(DELTA));
        assertThat(result.get("Recall")).isEqualTo(1.0, offset(DELTA));
        assertThat(result.get("F1")).isEqualTo(1.0, offset(DELTA));
    }

    @Test
    void testEmptyAccumulator() {
        ClassificationEvaluatorAccumulator accumulator = new ClassificationEvaluatorAccumulator();
        assertThat(accumulator.getValue()).isNull();
    }

    @Test
    void testInvalidInput() {
        ClassificationEvaluatorAccumulator accumulator = new ClassificationEvaluatorAccumulator();
        assertThatThrownBy(() -> accumulator.accumulate("A"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> accumulator.accumulate("A", "B", "C"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> accumulator.retract("A"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testMerge() {
        ClassificationEvaluatorAccumulator acc1 = new ClassificationEvaluatorAccumulator();
        ClassificationEvaluatorAccumulator acc2 = new ClassificationEvaluatorAccumulator();
        // Add predictions to the first accumulator
        acc1.accumulate("A", "A");
        acc1.accumulate("A", "B");
        // Add predictions to the second accumulator
        acc2.accumulate("B", "B");
        acc2.accumulate("B", "A");
        // Merge accumulators
        acc1.merge(acc2);
        Map<String, Double> result = acc1.getValue();
        assertThat(result).isNotNull();
        // Verify merged confusion matrix results
        assertThat(result.get("Accuracy")).isEqualTo(0.5, offset(DELTA));
        assertThat(result.get("Precision")).isEqualTo(0.5, offset(DELTA));
        assertThat(result.get("Recall")).isEqualTo(0.5, offset(DELTA));
        assertThat(result.get("F1")).isEqualTo(0.5, offset(DELTA));
    }

    @Test
    void testReset() {
        ClassificationEvaluatorAccumulator accumulator = new ClassificationEvaluatorAccumulator();
        // Add some predictions
        accumulator.accumulate("A", "A");
        accumulator.accumulate("B", "A");
        // Reset accumulator
        accumulator.reset();
        // Should return null after reset
        assertThat(accumulator.getValue()).isNull();
        // Should work correctly after reset
        accumulator.accumulate("A", "A");
        Map<String, Double> result = accumulator.getValue();
        assertThat(result).isNotNull();
        assertThat(result.get("Accuracy")).isEqualTo(1.0, offset(DELTA));
    }

    @Test
    void testInvalidMerge() {
        ClassificationEvaluatorAccumulator accumulator = new ClassificationEvaluatorAccumulator();
        RegressionEvaluatorAccumulator wrongType = new RegressionEvaluatorAccumulator();
        assertThatThrownBy(() -> accumulator.merge(wrongType))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
