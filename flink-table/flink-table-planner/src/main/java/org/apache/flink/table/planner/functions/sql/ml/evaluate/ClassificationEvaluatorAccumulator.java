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
import org.apache.flink.types.Row;

import org.apache.flink.shaded.guava33.com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * Accumulator for computing classification model evaluation metrics. This accumulator calculates
 * the following metrics based on a confusion matrix: 1. Accuracy: ACC = (TP + TN) / (TP + TN + FP +
 * FN) where: - TP: True Positives (correctly predicted positive cases) - TN: True Negatives
 * (correctly predicted negative cases) - FP: False Positives (incorrectly predicted positive cases)
 * - FN: False Negatives (incorrectly predicted negative cases) - Ranges from 0 (worst) to 1
 * (perfect classification) 2. Precision (Positive Predictive Value): P = TP / (TP + FP) where: -
 * Measures exactness: proportion of positive predictions that are correct - Ranges from 0 (all
 * positive predictions wrong) to 1 (all positive predictions correct) - High precision indicates
 * low false positive rate 3. Recall (Sensitivity, True Positive Rate): R = TP / (TP + FN) where: -
 * Measures completeness: proportion of actual positives correctly identified - Ranges from 0
 * (missed all positives) to 1 (found all positives) - High recall indicates low false negative rate
 * 4. F1 Score: F1 = 2 * (P * R) / (P + R) where: - Harmonic mean of precision and recall - Balances
 * precision and recall - Ranges from 0 (worst) to 1 (perfect precision and recall) For multi-class
 * classification: - Metrics are calculated per class (one-vs-rest approach) - Final metrics are
 * macro-averaged across all classes: Macro-Average = (1/C) * Σ(i=1 to C) metric_i where C is the
 * number of classes Confusion Matrix Structure: - M[i, j] represents count of instances from actual
 * class i predicted as class j - Diagonal elements M[i, i] are true positives for class i - Row
 * sums minus diagonal are false negatives for class i - Column sums minus diagonal are false
 * positives for class i
 */
@Internal
public class ClassificationEvaluatorAccumulator extends ModelEvaluatorAccumulator {
    // Confusion matrix: outer map key is actual class, inner map key is predicted class
    private final Map<String, Map<String, Integer>> confusionMatrix =
            new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    // Number of samples accumulated
    private int count = 0;

    /**
     * Accumulates new values for classification metrics calculation. Updates confusion matrix M[i,
     * j] where: - i is the actual class - j is the predicted class - M[i, j] += 1 for each new
     * sample
     *
     * @param args Array of arguments in the following order: args[0] - actual class label (String)
     *     args[1] - predicted class label (String)
     */
    @Override
    public void accumulate(Object... args) {
        if (args.length != 2) {
            throw new IllegalArgumentException(
                    "accumulate of ClassificationEvaluatorAccumulator requires 2 arguments");
        }
        String actual = (String) args[0];
        String predicted = (String) args[1];
        // Initialize map for actual class if it doesn't exist
        if (!confusionMatrix.containsKey(actual)) {
            confusionMatrix.put(actual, new TreeMap<>(String.CASE_INSENSITIVE_ORDER));
        }
        // Increment count for predicted class
        confusionMatrix.get(actual).merge(predicted, 1, Integer::sum);
        count++;
    }

    /**
     * Retracts previously accumulated values from the metrics calculation.
     *
     * @param args Array of arguments in the following order: args[0] - actual class label to
     *     retract (String) args[1] - predicted class label to retract (String)
     * @throws IllegalArgumentException if the number of arguments is not exactly 2
     */
    @Override
    public void retract(Object... args) {
        if (args.length != 2) {
            throw new IllegalArgumentException(
                    "retract of ClassificationEvaluatorAccumulator requires 2 arguments");
        }
        String actual = (String) args[0];
        String predicted = (String) args[1];
        if (confusionMatrix.containsKey(actual)) {
            confusionMatrix.get(actual).merge(predicted, -1, Integer::sum);
        }
        count--;
    }

    /**
     * Returns the computed classification metrics as a Row containing macro-averaged values. For
     * each class i: TP_i = M[i,i] FP_i = Σ(j≠i) M[j,i] FN_i = Σ(j≠i) M[i,j] Class-wise metrics:
     * Precision_i = TP_i / (TP_i + FP_i) Recall_i = TP_i / (TP_i + FN_i) Macro-averaged metrics:
     * Macro-Precision = (1/C) * Σ(i=1 to C) Precision_i Macro-Recall = (1/C) * Σ(i=1 to C) Recall_i
     * Macro-F1 = 2 * (Macro-Precision * Marco-Recall) / (Marco-Precision + Macro-Recall)
     *
     * @return A Row containing macro-averaged metrics: [Accuracy, Precision, Recall, F1]
     */
    @Override
    public Row getValue() {
        if (confusionMatrix.isEmpty()) {
            return null;
        }
        double accuracy = 0.0;
        double precision = 0.0;
        double recall = 0.0;
        double f1;
        // Calculate metrics for each class and average them
        for (Map.Entry<String, Map<String, Integer>> entry : confusionMatrix.entrySet()) {
            // True positives for current class
            int tp = entry.getValue().getOrDefault(entry.getKey(), 0);

            // Calculate false positives and false negatives
            int fp =
                    confusionMatrix.entrySet().stream()
                            .filter(e -> !e.getKey().equals(entry.getKey()))
                            .mapToInt(e -> e.getValue().getOrDefault(entry.getKey(), 0))
                            .sum(); // False positives
            int fn =
                    entry.getValue().entrySet().stream()
                            .filter(e -> !entry.getKey().equals(e.getKey()))
                            .mapToInt(Entry::getValue)
                            .sum(); // False negatives

            // Accumulate metrics for current class
            accuracy += (double) tp / count;
            if (tp + fp == 0) {
                precision += 0.0;
            } else {
                precision += (double) tp / (tp + fp);
            }
            if (tp + fn == 0) {
                recall += 0.0;
            } else {
                recall += (double) tp / (tp + fn);
            }
        }
        // Average metrics across all classes
        int numClasses = confusionMatrix.size();
        precision = precision / numClasses;
        recall = recall / numClasses;
        if (precision + recall == 0) {
            f1 = 0.0;
        } else {
            f1 = 2 * precision * recall / (precision + recall);
        }
        return Row.of(
                ImmutableMap.of(
                        "Accuracy", accuracy,
                        "Precision", precision,
                        "Recall", recall,
                        "F1", f1));
    }

    /**
     * Merges another ClassificationEvaluatorAccumulator into this one. This operation combines
     * confusion matrices from both accumulators. The merge process: - Combines confusion matrices
     * by adding corresponding cell counts - Preserves all unique class labels - Maintains count
     * totals
     *
     * @param other The other accumulator to merge into this one
     * @throws IllegalArgumentException if other is not a ClassificationEvaluatorAccumulator
     */
    @Override
    public void merge(ModelEvaluatorAccumulator other) {
        if (!(other instanceof ClassificationEvaluatorAccumulator)) {
            throw new IllegalArgumentException("Can only merge ClassificationEvaluatorAccumulator");
        }
        ClassificationEvaluatorAccumulator otherAcc = (ClassificationEvaluatorAccumulator) other;
        this.count += otherAcc.count;
        // Merge confusion matrices
        for (Map.Entry<String, Map<String, Integer>> entry : otherAcc.confusionMatrix.entrySet()) {
            String actualClass = entry.getKey();
            if (!this.confusionMatrix.containsKey(actualClass)) {
                this.confusionMatrix.put(actualClass, new TreeMap<>(String.CASE_INSENSITIVE_ORDER));
            }
            for (Map.Entry<String, Integer> predEntry : entry.getValue().entrySet()) {
                String predictedClass = predEntry.getKey();
                int count = predEntry.getValue();
                this.confusionMatrix.get(actualClass).merge(predictedClass, count, Integer::sum);
            }
        }
    }

    /**
     * Resets the accumulator to its initial state. This operation: - Clears the confusion matrix -
     * Resets the sample count - Removes all accumulated class statistics
     */
    @Override
    public void reset() {
        this.confusionMatrix.clear();
        this.count = 0;
    }
}
