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

/** Test for RegressionEvaluatorAccumulator. */
public class RegressionEvaluatorAccumulatorTest {
    private static final double DELTA = 1e-6;

    @Test
    void testPerfectPrediction() {
        RegressionEvaluatorAccumulator accumulator = new RegressionEvaluatorAccumulator();
        // Perfect predictions
        accumulator.accumulate(1.0, 1.0);
        accumulator.accumulate(2.0, 2.0);
        accumulator.accumulate(3.0, 3.0);
        Map<String, Double> result = accumulator.getValue();
        assertThat(result).isNotNull();
        // All errors should be 0, R² should be 1
        assertThat(result.get("MAE")).isEqualTo(0.0, offset(DELTA));
        assertThat(result.get("MSE")).isEqualTo(0.0, offset(DELTA));
        assertThat(result.get("RMSE")).isEqualTo(0.0, offset(DELTA));
        assertThat(result.get("MAPE")).isEqualTo(0.0, offset(DELTA));
        assertThat(result.get("R2")).isEqualTo(1.0, offset(DELTA));
    }

    @Test
    void testTypicalPredictions() {
        RegressionEvaluatorAccumulator accumulator = new RegressionEvaluatorAccumulator();
        // Predictions with known errors
        accumulator.accumulate(1.0, 1.2); // error = -0.2
        accumulator.accumulate(2.0, 1.8); // error = 0.2
        accumulator.accumulate(3.0, 2.8); // error = 0.2
        Map<String, Double> result = accumulator.getValue();
        assertThat(result).isNotNull();
        assertThat(result.get("MAE")).isEqualTo(0.2, offset(DELTA));
        assertThat(result.get("MSE")).isEqualTo(0.04, offset(DELTA));
        assertThat(result.get("RMSE")).isEqualTo(0.2, offset(DELTA));
        assertThat(result.get("R2")).isLessThan(1.0);
    }

    @Test
    void testRetraction() {
        RegressionEvaluatorAccumulator accumulator = new RegressionEvaluatorAccumulator();
        // Add some values
        accumulator.accumulate(1.0, 1.2);
        accumulator.accumulate(2.0, 1.8);
        // Retract one value
        accumulator.retract(1.0, 1.2);
        Map<String, Double> result = accumulator.getValue();
        assertThat(result).isNotNull();
        // Should only reflect the remaining prediction
        assertThat(result.get("MAE")).isEqualTo(0.2, offset(DELTA));
        assertThat(result.get("MSE")).isEqualTo(0.04, offset(DELTA));
    }

    @Test
    void testEmptyAccumulator() {
        RegressionEvaluatorAccumulator accumulator = new RegressionEvaluatorAccumulator();
        assertThat(accumulator.getValue()).isNull();
    }

    @Test
    void testInvalidInput() {
        RegressionEvaluatorAccumulator accumulator = new RegressionEvaluatorAccumulator();
        assertThatThrownBy(() -> accumulator.accumulate(1.0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> accumulator.accumulate(1.0, 2.0, 3.0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> accumulator.retract(1.0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testMerge() {
        RegressionEvaluatorAccumulator acc1 = new RegressionEvaluatorAccumulator();
        RegressionEvaluatorAccumulator acc2 = new RegressionEvaluatorAccumulator();
        // Add values to first accumulator
        acc1.accumulate(1.0, 1.2); // error = -0.2
        acc1.accumulate(2.0, 1.8); // error = 0.2
        // Add values to second accumulator
        acc2.accumulate(3.0, 2.8); // error = 0.2
        // Merge accumulators
        acc1.merge(acc2);
        Map<String, Double> result = acc1.getValue();
        assertThat(result).isNotNull();
        // Verify merged results match the expected values for all points
        assertThat(result.get("MAE")).isEqualTo(0.2, offset(DELTA));
        assertThat(result.get("MSE")).isEqualTo(0.04, offset(DELTA));
        assertThat(result.get("RMSE")).isEqualTo(0.2, offset(DELTA));
    }

    @Test
    void testReset() {
        RegressionEvaluatorAccumulator accumulator = new RegressionEvaluatorAccumulator();
        // Add some values
        accumulator.accumulate(1.0, 1.2);
        accumulator.accumulate(2.0, 1.8);
        // Reset accumulator
        accumulator.reset();
        // Should return null after reset
        assertThat(accumulator.getValue()).isNull();
        // Should work correctly after reset
        accumulator.accumulate(3.0, 3.0);
        Map<String, Double> result = accumulator.getValue();
        assertThat(result).isNotNull();
        assertThat(result.get("MAE")).isEqualTo(0.0, offset(DELTA));
    }

    @Test
    void testInvalidMerge() {
        RegressionEvaluatorAccumulator accumulator = new RegressionEvaluatorAccumulator();
        ClassificationEvaluatorAccumulator wrongType = new ClassificationEvaluatorAccumulator();
        assertThatThrownBy(() -> accumulator.merge(wrongType))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
