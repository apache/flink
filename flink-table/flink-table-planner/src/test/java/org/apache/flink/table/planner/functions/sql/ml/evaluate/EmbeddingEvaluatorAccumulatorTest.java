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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.offset;

/** Test for EmbeddingEvaluatorAccumulator. */
public class EmbeddingEvaluatorAccumulatorTest {
    private static final double DELTA = 1e-10;
    private EmbeddingEvaluatorAccumulator accumulator;

    @BeforeEach
    public void setUp() {
        accumulator = new EmbeddingEvaluatorAccumulator();
    }

    @Test
    public void testPerfectMatch() {
        // Test with identical vectors
        Float[] actual = new Float[] {1.0f, 0.0f, 0.0f};
        Float[] predicted = new Float[] {1.0f, 0.0f, 0.0f};

        accumulator.accumulate(actual, predicted);
        Map<String, Double> result = accumulator.getValue();

        assertThat(result.get("MCS")).isEqualTo(1.0, offset(DELTA));
        assertThat(result.get("MJS")).isEqualTo(1.0, offset(DELTA));
        assertThat(result.get("MED")).isEqualTo(0.0, offset(DELTA));
    }

    @Test
    public void testOrthogonalVectors() {
        // Test with orthogonal vectors (90 degrees apart)
        Float[] actual = new Float[] {0.0f, 1.0f};
        Float[] predicted = new Float[] {1.0f, 0.0f};

        accumulator.accumulate(actual, predicted);
        Map<String, Double> result = accumulator.getValue();

        assertThat(result.get("MCS")).isEqualTo(0.0, offset(DELTA));
        assertThat(result.get("MJS")).isEqualTo(0.0, offset(DELTA));
        assertThat(result.get("MED")).isEqualTo(Math.sqrt(2.0), offset(DELTA));
    }

    @Test
    public void testOppositeVectors() {
        // Test with opposite vectors (180 degrees apart)
        Float[] actual = new Float[] {-1.0f, 0.0f};
        Float[] predicted = new Float[] {1.0f, 0.0f};

        accumulator.accumulate(actual, predicted);
        Map<String, Double> result = accumulator.getValue();
        assertThat(result).isNotNull();
        assertThat(result.get("MED")).isEqualTo(2.0, offset(DELTA));
    }

    @Test
    public void testMultipleAccumulations() {
        // Test with multiple vector pairs
        Float[] predicted1 = new Float[] {1.0f, 0.0f};
        Float[] actual1 = new Float[] {1.0f, 0.0f};
        Float[] predicted2 = new Float[] {0.0f, 1.0f};
        Float[] actual2 = new Float[] {0.0f, 1.0f};

        accumulator.accumulate(actual1, predicted1);
        accumulator.accumulate(actual2, predicted2);
        Map<String, Double> result = accumulator.getValue();

        assertThat(result.get("MCS")).isEqualTo(1.0, offset(DELTA));
        assertThat(result.get("MJS")).isEqualTo(1.0, offset(DELTA));
        assertThat(result.get("MED")).isEqualTo(0.0, offset(DELTA));
    }

    @Test
    public void testRetraction() {
        // Test accumulation and retraction
        Float[] predicted1 = new Float[] {1.0f, 0.0f};
        Float[] actual1 = new Float[] {1.0f, 0.0f};
        Float[] predicted2 = new Float[] {0.0f, 1.0f};
        Float[] actual2 = new Float[] {0.0f, 1.0f};

        accumulator.accumulate(actual1, predicted1);
        accumulator.accumulate(actual2, predicted2);
        accumulator.retract(actual2, predicted2);
        Map<String, Double> result = accumulator.getValue();

        assertThat(result.get("MCS")).isEqualTo(1.0, offset(DELTA));
        assertThat(result.get("MJS")).isEqualTo(1.0, offset(DELTA));
        assertThat(result.get("MED")).isEqualTo(0.0, offset(DELTA));
    }

    @Test
    public void testEmptyAccumulator() {
        // Test with no accumulated values
        Map<String, Double> result = accumulator.getValue();
        assertThat(result).isNull();
    }

    @Test
    public void testNonNegativeVectors() {
        // Test with non-negative vectors
        Float[] actual = new Float[] {0.4f, 0.4f, 0.2f};
        Float[] predicted = new Float[] {0.5f, 0.3f, 0.2f};

        accumulator.accumulate(actual, predicted);
        Map<String, Double> result = accumulator.getValue();

        assertThat(result.get("MCS")).isPositive();
        assertThat(result.get("MJS")).isPositive();
        assertThat(result.get("MED")).isNotNegative();
    }

    @Test
    public void testInvalidInput() {
        double[] embedding = new double[] {1.0};
        assertThatThrownBy(() -> accumulator.accumulate(embedding))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> accumulator.accumulate(embedding, embedding, embedding))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> accumulator.retract(embedding))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testDifferentDimensions() {
        Float[] embedding1 = new Float[] {1.0F, 0.0F};
        Float[] embedding2 = new Float[] {1.0F};
        assertThatThrownBy(() -> accumulator.accumulate(embedding1, embedding2))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testMerge() {
        EmbeddingEvaluatorAccumulator acc1 = new EmbeddingEvaluatorAccumulator();
        EmbeddingEvaluatorAccumulator acc2 = new EmbeddingEvaluatorAccumulator();
        // Add embeddings to first accumulator
        acc1.accumulate(new Float[] {1.0F, 0.0F}, new Float[] {1.0F, 0.0F});
        // Add embeddings to second accumulator
        acc2.accumulate(new Float[] {0.0F, 1.0F}, new Float[] {0.0F, 1.0F});
        // Merge accumulators
        acc1.merge(acc2);
        Map<String, Double> result = acc1.getValue();
        assertThat(result).isNotNull();
        // Verify merged similarity scores
        assertThat(result.get("MCS")).isEqualTo(1.0, offset(DELTA));
        assertThat(result.get("MJS")).isEqualTo(1.0, offset(DELTA));
        assertThat(result.get("MED")).isEqualTo(0.0, offset(DELTA));
    }

    @Test
    public void testReset() {
        // Add some embeddings
        Float[] predict = new Float[] {1.0F, 0.0F};
        Float[] actual = new Float[] {0.0F, 1.0F};
        accumulator.accumulate(actual, predict);
        // Reset accumulator
        accumulator.reset();
        // Should return null after reset
        assertThat(accumulator.getValue()).isNull();
        // Should work correctly after reset
        accumulator.accumulate(actual, actual);
        Map<String, Double> result = accumulator.getValue();
        assertThat(result).isNotNull();
        assertThat(result.get("MCS")).isEqualTo(1.0, offset(DELTA));
    }

    @Test
    public void testInvalidMerge() {
        RegressionEvaluatorAccumulator wrongType = new RegressionEvaluatorAccumulator();
        assertThatThrownBy(() -> accumulator.merge(wrongType))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testNegativeVectors() {
        Float[] v1 = new Float[] {1.0f, -1.0f};
        Float[] v2 = new Float[] {-1.0f, 1.0f};

        accumulator.accumulate(v1, v2);
        Map<String, Double> result = accumulator.getValue();

        assertThat(result.get("MCS")).isEqualTo(-1.0, offset(DELTA));
        assertThat(result.get("MJS")).isEqualTo(0.0, offset(DELTA));
        assertThat(result.get("MED")).isEqualTo(Math.sqrt(8), offset(DELTA));
    }
}
