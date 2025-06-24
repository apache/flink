/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

/** Test for TextGenerationEvaluatorAccumulator. */
public class TextGenerationEvaluatorAccumulatorTest {
    private static final double DELTA = 1e-6;
    private TextGenerationEvaluatorAccumulator accumulator;

    @BeforeEach
    void setUp() {
        accumulator = new TextGenerationEvaluatorAccumulator();
    }

    @Test
    void testIdenticalTexts() {
        String text = "The quick brown fox jumps over the lazy dog";
        accumulator.accumulate(text, text);
        Map<String, Double> result = accumulator.getValue();
        assertThat(result).isNotNull();
        assertThat(result.get("MB")).isEqualTo(1.0, offset(DELTA));
        assertThat(result.get("MR")).isEqualTo(1.0, offset(DELTA));
        assertThat(result.get("MSS")).isEqualTo(1.0, offset(DELTA));
    }

    @Test
    void testSimilarTexts() {
        String reference = "The quick brown fox jumps over the lazy dog";
        String hypothesis = "A quick brown fox jumped over a lazy dog";
        accumulator.accumulate(reference, hypothesis);
        Map<String, Double> result = accumulator.getValue();
        assertThat(result).isNotNull();
        assertThat(result.get("MB")).isGreaterThan(0.5);
        assertThat(result.get("MR")).isGreaterThan(0.5);
        assertThat(result.get("MSS")).isGreaterThan(0.5);
    }

    @Test
    void testDifferentTexts() {
        String reference = "The quick brown fox jumps over the lazy dog";
        String hypothesis = "A cat sleeps on the windowsill";
        accumulator.accumulate(reference, hypothesis);
        Map<String, Double> result = accumulator.getValue();
        assertThat(result).isNotNull();
        assertThat(result.get("MB")).isLessThan(0.3);
        assertThat(result.get("MR")).isLessThan(0.3);
        assertThat(result.get("MSS")).isLessThan(0.3);
    }

    @Test
    void testRetraction() {
        String text1 = "The quick brown fox";
        String text2 = "A quick brown fox";
        accumulator.accumulate(text1, text2);
        accumulator.accumulate(text1, text1);
        accumulator.retract(text1, text2);
        Map<String, Double> result = accumulator.getValue();
        assertThat(result).isNotNull();
        assertThat(result.get("MB")).isEqualTo(1.0, offset(DELTA));
        assertThat(result.get("MR")).isEqualTo(1.0, offset(DELTA));
        assertThat(result.get("MSS")).isEqualTo(1.0, offset(DELTA));
    }

    @Test
    void testEmptyAccumulator() {
        assertThat(accumulator.getValue()).isNull();
    }

    @Test
    void testInvalidInput() {
        assertThatThrownBy(() -> accumulator.accumulate("text"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> accumulator.accumulate("text1", "text2", "text3"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> accumulator.retract("text"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testMerge() {
        TextGenerationEvaluatorAccumulator acc2 = new TextGenerationEvaluatorAccumulator();
        // Add texts to the first accumulator
        accumulator.accumulate("The quick brown fox", "The quick brown fox");
        // Add texts to the second accumulator
        acc2.accumulate("jumps over the lazy dog", "jumps over the lazy dog");
        // Merge accumulators
        accumulator.merge(acc2);
        Map<String, Double> result = accumulator.getValue();
        assertThat(result).isNotNull();
        // Verify merged scores
        assertThat(result.get("MB")).isEqualTo(1.0, offset(DELTA));
        assertThat(result.get("MR")).isEqualTo(1.0, offset(DELTA));
        assertThat(result.get("MSS")).isEqualTo(1.0, offset(DELTA));
    }

    @Test
    void testReset() {
        // Add some texts
        accumulator.accumulate("A quick brown fox", "The quick brown fox");
        // Reset accumulator
        accumulator.reset();
        // Should return null after reset
        assertThat(accumulator.getValue()).isNull();
        // Should work correctly after reset
        String text = "The quick brown fox";
        accumulator.accumulate(text, text);
        Map<String, Double> result = accumulator.getValue();
        assertThat(result).isNotNull();
        assertThat(result.get("MB")).isEqualTo(1.0, offset(DELTA));
        assertThat(result.get("MR")).isEqualTo(1.0, offset(DELTA));
        assertThat(result.get("MSS")).isEqualTo(1.0, offset(DELTA));
    }

    @Test
    void testInvalidMerge() {
        RegressionEvaluatorAccumulator wrongType = new RegressionEvaluatorAccumulator();
        assertThatThrownBy(() -> accumulator.merge(wrongType))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
