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

import java.util.Map;

/**
 * Accumulator for computing embedding model evaluation metrics.
 *
 * <p>This accumulator calculates the following similarity/distance metrics between embeddings:
 *
 * <h3>1. Cosine Similarity</h3>
 *
 * <p>Formula: cos(θ) = (v1 · v2) / (||v1|| ||v2||)
 *
 * <ul>
 *   <li>v1 · v2 is the dot product: Σ(v1_i * v2_i)
 *   <li>||v|| is the L2 norm: √(Σv_i²)
 *   <li>Ranges from -1 (opposite direction) to 1 (same direction)
 *   <li>Value of 0 indicates orthogonality (90° angle)
 * </ul>
 *
 * <h3>2. Jaccard Similarity</h3>
 *
 * <p>Formula: J(v1,v2) = Σ_min(v1_i, v2_i) / Σ_max(v1_i, v2_i)
 *
 * <ul>
 *   <li>Treats vectors as fuzzy sets
 *   <li>Numerator represents intersection
 *   <li>Denominator represents union
 *   <li>Ranges from 0 (no overlap) to 1 (identical)
 * </ul>
 *
 * <h3>3. Euclidean Distance</h3>
 *
 * <p>Formula: d(v1,v2) = √(Σ(v1_i - v2_i)²)
 *
 * <ul>
 *   <li>Measures straight-line distance in vector space
 *   <li>Always non-negative
 *   <li>Value of 0 indicates identical vectors
 *   <li>Sensitive to magnitude differences
 * </ul>
 *
 * <p>The accumulator supports both accumulation of new values and retraction of existing values,
 * making it suitable for streaming scenarios with updates and retractions.
 */
@Internal
public class EmbeddingEvaluatorAccumulator extends ModelEvaluatorAccumulator {
    private static final long serialVersionUID = 1L;

    // Accumulated sum of cosine similarity scores between embeddings
    private double totalCosineSimilarity = 0.0;
    // Accumulated sum of Jaccard similarity scores between embeddings
    private double totalJaccardSimilarity = 0.0;
    // Accumulated sum of Euclidean distances between embeddings
    private double totalEuclideanDistance = 0.0;
    // Number of samples accumulated
    private int count = 0;

    /**
     * Accumulates new embedding vectors and computes their similarity/distance metrics.
     *
     * @param args Array of arguments in the following order: args[0] - predicted embedding vector
     *     (double[]) args[1] - actual embedding vector (double[])
     * @throws IllegalArgumentException if the number of arguments is not exactly 2 or if the
     *     vectors have different lengths
     */
    @Override
    public void accumulate(Object... args) {
        if (args.length != 2) {
            throw new IllegalArgumentException(
                    "accumulate of EmbeddingEvaluatorAccumulator requires 2 arguments");
        }

        // Skip accumulate if any argument is null
        if (args[0] == null || args[1] == null) {
            return;
        }

        Float[] actual = getFloatArray(args[0]);
        Float[] predicted = getFloatArray(args[1]);
        if (predicted.length != actual.length) {
            throw new IllegalArgumentException("Embedding vectors must have the same length");
        }
        totalCosineSimilarity += calculateCosineSimilarity(predicted, actual);
        totalJaccardSimilarity += calculateJaccardSimilarity(predicted, actual);
        totalEuclideanDistance += calculateEuclideanDistance(predicted, actual);
        count++;
    }

    /**
     * Retracts previously accumulated embedding vectors and their metrics.
     *
     * @param args Array of arguments in the following order: args[0] - predicted embedding vector
     *     to retract (double[]) args[1] - actual embedding vector to retract (double[])
     * @throws IllegalArgumentException if the number of arguments is not exactly 2 or if the
     *     vectors have different lengths
     */
    @Override
    public void retract(Object... args) {
        if (args.length != 2) {
            throw new IllegalArgumentException(
                    "retract of EmbeddingEvaluatorAccumulator requires 2 arguments");
        }

        // Skip retract if any argument is null
        if (args[0] == null || args[1] == null) {
            return;
        }

        Float[] actual = getFloatArray(args[0]);
        Float[] predicted = getFloatArray(args[1]);
        if (predicted.length != actual.length) {
            throw new IllegalArgumentException("Embedding vectors must have the same length");
        }
        totalCosineSimilarity -= calculateCosineSimilarity(predicted, actual);
        totalJaccardSimilarity -= calculateJaccardSimilarity(predicted, actual);
        totalEuclideanDistance -= calculateEuclideanDistance(predicted, actual);
        count--;
    }

    /**
     * Calculates the cosine similarity between two vectors. Formula: cos(θ) = (v1 · v2) / (||v1||
     * ||v2||) where: - v1 · v2 = Σ(v1_i * v2_i) [dot product] - ||v|| = √(Σv_i²) [L2 norm]
     *
     * @param v1 First vector
     * @param v2 Second vector
     * @return Cosine similarity in range [-1, 1]
     */
    private double calculateCosineSimilarity(Float[] v1, Float[] v2) {
        if (isIdenticalArray(v1, v2)) {
            return 1.0; // Return 1 if vectors are identical
        }

        double dotProduct = 0.0;
        double norm1 = 0.0;
        double norm2 = 0.0;
        for (int i = 0; i < v1.length; i++) {
            dotProduct += v1[i] * v2[i];
            norm1 += v1[i] * v1[i];
            norm2 += v2[i] * v2[i];
        }
        return dotProduct / (Math.sqrt(norm1) * Math.sqrt(norm2));
    }

    /**
     * Calculates the Jaccard similarity between two vectors. Formula: J(v1,v2) = Σ_min(v1_i, v2_i)
     * / Σ_max(v1_i, v2_i) Properties: - Treats vectors as fuzzy sets - Handles non-binary values -
     * Symmetric: J(v1,v2) = J(v2,v1) - Range [0,1]: 0 for no overlap, 1 for identical vectors
     *
     * @param v1 First vector
     * @param v2 Second vector
     * @return Jaccard similarity in range [0, 1]
     */
    private double calculateJaccardSimilarity(Float[] v1, Float[] v2) {
        if (isIdenticalArray(v1, v2)) {
            return 1.0; // Return 1 if vectors are identical
        }

        double minValue = findMin(v1, v2);
        double intersection = 0.0;
        double union = 0.0;
        // Shift values to ensure non-negative
        for (int i = 0; i < v1.length; i++) {
            intersection += Math.min(v1[i], v2[i]) - (minValue < 0 ? minValue : 0);
            union += Math.max(v1[i], v2[i]) - (minValue < 0 ? minValue : 0);
        }
        return intersection / union;
    }

    private double findMin(Float[] v1, Float[] v2) {
        double min = Double.MAX_VALUE;
        for (int i = 0; i < v1.length; i++) {
            min = Math.min(min, Math.min(v1[i], v2[i]));
        }
        return min;
    }

    /**
     * Calculates the Euclidean distance between two vectors. Formula: d(v1,v2) = √(Σ(v1_i - v2_i)²)
     * Properties: - Satisfies triangle inequality: d(x,z) ≤ d(x,y) + d(y,z) - Symmetric: d(v1,v2) =
     * d(v2,v1) - Non-negative: d(v1,v2) ≥ 0 - Identity: d(v1,v2) = 0 iff v1 = v2
     *
     * @param v1 First vector
     * @param v2 Second vector
     * @return Euclidean distance (≥ 0)
     */
    private double calculateEuclideanDistance(Float[] v1, Float[] v2) {
        if (isIdenticalArray(v1, v2)) {
            return 0.0; // Return 0 if vectors are identical
        }

        double sum = 0.0;
        for (int i = 0; i < v1.length; i++) {
            double diff = v1[i] - v2[i];
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }

    /**
     * Returns the computed embedding metrics as a Row containing mean values. For n pairs of
     * vectors, calculates: - Mean Cosine Similarity = (1/n) * Σcos(θ_i) - Mean Jaccard Similarity =
     * (1/n) * ΣJ_i - Mean Euclidean Distance = (1/n) * Σd_i
     *
     * @return A Row containing the following metrics (in order): - Mean Cosine Similarity: Average
     *     similarity based on angle between vectors - Mean Jaccard Similarity: Average similarity
     *     based on set intersection/union - Mean Euclidean Distance: Average straight-line distance
     *     between embeddings Returns null if no samples have been accumulated (count = 0)
     */
    @Override
    public Map<String, Double> getValue() {
        if (count == 0) {
            return null;
        }
        return ImmutableMap.of(
                "MCS", // Mean Cosine Similarity
                totalCosineSimilarity / count,
                "MJS", // Mean Jaccard Similarity
                totalJaccardSimilarity / count,
                "MED", // Mean Euclidean Distance
                totalEuclideanDistance / count);
    }

    /**
     * Merges another EmbeddingEvaluatorAccumulator into this one. This operation combines
     * similarity and distance metrics from both accumulators. The merge process: - Adds accumulated
     * similarity scores - Adds accumulated distances - Combines sample counts
     *
     * @param other The other accumulator to merge into this one
     * @throws IllegalArgumentException if other is not an EmbeddingEvaluatorAccumulator
     */
    @Override
    public void merge(ModelEvaluatorAccumulator other) {
        if (!(other instanceof EmbeddingEvaluatorAccumulator)) {
            throw new IllegalArgumentException("Can only merge EmbeddingEvaluatorAccumulator");
        }
        EmbeddingEvaluatorAccumulator otherAcc = (EmbeddingEvaluatorAccumulator) other;
        this.totalCosineSimilarity += otherAcc.totalCosineSimilarity;
        this.totalJaccardSimilarity += otherAcc.totalJaccardSimilarity;
        this.totalEuclideanDistance += otherAcc.totalEuclideanDistance;
        this.count += otherAcc.count;
    }

    /**
     * Resets the accumulator to its initial state. This operation: - Clears all accumulated
     * similarity scores - Resets distance measurements - Zeros the sample count
     */
    @Override
    public void reset() {
        this.totalCosineSimilarity = 0.0;
        this.totalJaccardSimilarity = 0.0;
        this.totalEuclideanDistance = 0.0;
        this.count = 0;
    }

    private static Float[] getFloatArray(Object obj) {
        Object[] arr = (Object[]) obj;
        Float[] floatArr = new Float[arr.length];
        for (int i = 0; i < arr.length; i++) {
            if (arr[i] instanceof Number) {
                floatArr[i] = ((Number) arr[i]).floatValue();
            } else {
                throw new IllegalArgumentException("Array elements must be numbers");
            }
        }
        return floatArr;
    }

    private static boolean isIdenticalArray(Float[] arr1, Float[] arr2) {
        if (arr1.length != arr2.length) {
            return false;
        }
        for (int i = 0; i < arr1.length; i++) {
            if (!arr1[i].equals(arr2[i])) {
                return false;
            }
        }
        return true;
    }
}
