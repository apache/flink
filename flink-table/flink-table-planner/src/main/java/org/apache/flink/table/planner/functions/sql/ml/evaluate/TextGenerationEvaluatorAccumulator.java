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

import org.apache.flink.annotation.Internal;

import org.apache.flink.shaded.guava33.com.google.common.collect.ImmutableMap;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Accumulator for computing text generation model evaluation metrics.
 *
 * <p>This accumulator calculates the following metrics:
 *
 * <ul>
 *   <li>BLEU Score: Bilingual Evaluation Understudy score for evaluating generated text quality
 *   <li>ROUGE Score: Recall-Oriented Understudy for Gisting Evaluation score
 *   <li>Semantic Similarity: Measure of semantic similarity between generated and reference text
 * </ul>
 *
 * <p>The accumulator supports both accumulation of new values and retraction of existing values,
 * making it suitable for streaming scenarios with updates and retractions.
 */
@Internal
public class TextGenerationEvaluatorAccumulator extends ModelEvaluatorAccumulator {
    private static final long serialVersionUID = 1L;

    // Accumulated sum of BLEU scores
    private double totalBleuScore = 0.0;
    // Accumulated sum of ROUGE scores
    private double totalRougeScore = 0.0;
    // Accumulated sum of semantic similarity scores
    private double totalSemanticSimilarity = 0.0;
    // Number of samples accumulated
    private int count = 0;

    /**
     * Accumulates new text pairs and computes their evaluation metrics.
     *
     * @param args Array of arguments in the following order: args[0] - predicted text (String)
     *     args[1] - actual text (String)
     * @throws IllegalArgumentException if the number of arguments is not exactly 2
     */
    @Override
    public void accumulate(Object... args) {
        if (args.length != 2) {
            throw new IllegalArgumentException(
                    "accumulate of TextGenerationEvaluatorAccumulator requires 2 arguments");
        }

        // Skip accumulate if any argument is null
        if (args[0] == null || args[1] == null) {
            return;
        }

        String actual = ((String) args[0]).toLowerCase();
        String predicted = ((String) args[1]).toLowerCase();
        totalBleuScore += calculateBLEUScore(actual, predicted);
        totalRougeScore += calculateROUGEScore(actual, predicted);
        totalSemanticSimilarity += calculateSemanticSimilarity(actual, predicted);
        count++;
    }

    /**
     * Retracts previously accumulated text pairs and their metrics.
     *
     * @param args Array of arguments in the following order: args[0] - predicted text to retract
     *     (String) args[1] - actual text to retract (String)
     * @throws IllegalArgumentException if the number of arguments is not exactly 2
     */
    @Override
    public void retract(Object... args) {
        if (args.length != 2) {
            throw new IllegalArgumentException(
                    "retract of TextGenerationEvaluatorAccumulator requires 2 arguments");
        }

        // Skip retract if any argument is null
        if (args[0] == null || args[1] == null) {
            return;
        }

        String actual = ((String) args[0]).toLowerCase();
        String predicted = ((String) args[1]).toLowerCase();
        totalBleuScore -= calculateBLEUScore(actual, predicted);
        totalRougeScore -= calculateROUGEScore(actual, predicted);
        totalSemanticSimilarity -= calculateSemanticSimilarity(actual, predicted);
        count--;
    }

    /**
     * Calculates BLEU score between reference and hypothesis texts. BLEU (Bilingual Evaluation
     * Understudy) score is calculated as: BLEU = BP * exp(sum(w_n * log(p_n))) where: - BP =
     * brevity penalty = exp(1 - max(r/h, 1)) - r = reference length - h = hypothesis length - p_n =
     * modified n-gram precision This implementation uses a simplified unigram (n=1) version: BLEU =
     * BP * (matching_unigrams / total_hypothesis_unigrams)
     *
     * @param reference The reference (ground truth) text
     * @param hypothesis The generated (predicted) text
     * @return BLEU score between 0 and 1, where higher values indicate better matches
     */
    private double calculateBLEUScore(String reference, String hypothesis) {
        String[] refWords = reference.split("\\s+");
        String[] hypWords = hypothesis.split("\\s+");
        Map<String, Integer> refCounts = new HashMap<>();
        for (String word : refWords) {
            refCounts.merge(word, 1, Integer::sum);
        }
        Map<String, Integer> hypCounts = new HashMap<>();
        for (String word : hypWords) {
            hypCounts.merge(word, 1, Integer::sum);
        }
        double matches = 0;
        for (Map.Entry<String, Integer> entry : hypCounts.entrySet()) {
            matches += Math.min(entry.getValue(), refCounts.getOrDefault(entry.getKey(), 0));
        }
        // Apply brevity penalty: BP = exp(1 - max(r/h, 1))
        double brevityPenalty =
                Math.exp(1 - Math.max((double) refWords.length / hypWords.length, 1.0));
        return brevityPenalty * matches / hypWords.length;
    }

    /**
     * Calculates ROUGE score between reference and hypothesis texts. ROUGE-1 (Recall-Oriented
     * Understudy for Gisting Evaluation) is calculated as: ROUGE-1 = (matching_unigrams) /
     * (total_reference_unigrams) This implementation uses ROUGE-1 recall, which measures: - How
     * many of the reference words appear in the hypothesis - Accounts for word repetition by using
     * word counts - Higher values indicate better coverage of reference content
     *
     * @param reference The reference (ground truth) text
     * @param hypothesis The generated (predicted) text
     * @return ROUGE-1 recall score between 0 and 1, where higher values indicate better recall
     */
    private double calculateROUGEScore(String reference, String hypothesis) {
        String[] refWords = reference.split("\\s+");
        String[] hypWords = hypothesis.split("\\s+");
        Map<String, Integer> refCounts = new HashMap<>();
        for (String word : refWords) {
            refCounts.merge(word, 1, Integer::sum);
        }
        double matches = 0;
        for (String word : hypWords) {
            if (refCounts.containsKey(word) && refCounts.get(word) > 0) {
                matches++;
                refCounts.put(word, refCounts.get(word) - 1);
            }
        }
        return matches / refWords.length;
    }

    /**
     * Calculates semantic similarity between two texts using Jaccard similarity. Jaccard Similarity
     * is calculated as: J(A,B) = |A ∩ B| / |A ∪ B| where: - A, B are sets of unique words from each
     * text - |A ∩ B| is the size of intersection (common words) - |A ∪ B| is the size of union (all
     * unique words) Properties: - Ranges from 0 (no overlap) to 1 (identical word sets) - Ignores
     * word order and frequency - Case-insensitive comparison - Treats texts as bags of words
     *
     * @param text1 First text for comparison
     * @param text2 Second text for comparison
     * @return Jaccard similarity score between 0 and 1, where higher values indicate more word
     *     overlap
     */
    private double calculateSemanticSimilarity(String text1, String text2) {
        Set<String> words1 = new HashSet<>(Arrays.asList(text1.split("\\s+")));
        Set<String> words2 = new HashSet<>(Arrays.asList(text2.split("\\s+")));
        Set<String> intersection = new HashSet<>(words1);
        intersection.retainAll(words2);
        Set<String> union = new HashSet<>(words1);
        union.addAll(words2);
        return (double) intersection.size() / union.size();
    }

    /**
     * Returns the computed text generation metrics as a Row containing mean values.
     *
     * @return A Row containing the following metrics (in order): - Mean BLEU Score - Mean ROUGE
     *     Score - Mean Semantic Similarity Score Returns null if no samples have been accumulated
     *     (count = 0)
     */
    @Override
    public Map<String, Double> getValue() {
        if (count == 0) {
            return null;
        }
        return ImmutableMap.of(
                "MB", // Mean BLEU Score
                totalBleuScore / count,
                "MR", // Mean ROUGE Score
                totalRougeScore / count,
                "MSS", // Mean Semantic Similarity Score
                totalSemanticSimilarity / count);
    }

    /**
     * Merges another TextGenerationEvaluatorAccumulator into this one. This operation combines text
     * evaluation metrics from both accumulators. The merge process: - Combines BLEU scores -
     * Combines ROUGE scores - Combines semantic similarity scores - Adds sample counts
     *
     * @param other The other accumulator to merge into this one
     * @throws IllegalArgumentException if other is not a TextGenerationEvaluatorAccumulator
     */
    @Override
    public void merge(ModelEvaluatorAccumulator other) {
        if (!(other instanceof TextGenerationEvaluatorAccumulator)) {
            throw new IllegalArgumentException("Can only merge TextGenerationEvaluatorAccumulator");
        }
        TextGenerationEvaluatorAccumulator otherAcc = (TextGenerationEvaluatorAccumulator) other;
        this.totalBleuScore += otherAcc.totalBleuScore;
        this.totalRougeScore += otherAcc.totalRougeScore;
        this.totalSemanticSimilarity += otherAcc.totalSemanticSimilarity;
        this.count += otherAcc.count;
    }

    /**
     * Resets the accumulator to its initial state. This operation: - Clears all accumulated scores
     * (BLEU, ROUGE, semantic similarity) - Resets the sample count - Prepares accumulator for new
     * text evaluation
     */
    @Override
    public void reset() {
        this.totalBleuScore = 0.0;
        this.totalRougeScore = 0.0;
        this.totalSemanticSimilarity = 0.0;
        this.count = 0;
    }
}
