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

/**
 * Accumulator for computing regression model evaluation metrics. This accumulator calculates the
 * following metrics: 1. Mean Absolute Error (MAE): MAE = (1/n) * Σ|y_i - ŷ_i| where: - n is the
 * number of samples - y_i is the actual value - ŷ_i is the predicted value 2. Mean Squared Error
 * (MSE): MSE = (1/n) * Σ(y_i - ŷ_i)² where: - Squares errors to penalize larger errors more heavily
 * - Always positive due to squaring 3. Root Mean Squared Error (RMSE): RMSE = √[(1/n) * Σ(y_i -
 * ŷ_i)²] where: - Square root of MSE - Same units as the original data 4. Mean Absolute Percentage
 * Error (MAPE): MAPE = (100/n) * Σ|((y_i - ŷ_i)/y_i)| where: - Expresses error as percentage - Only
 * defined for y_i ≠ 0 5. R-squared (R²): R² = 1 - (SSres/SStot) where: - SSres = Σ(y_i - ŷ_i)²
 * (Residual sum of squares) - SStot = Σ(y_i - ȳ)² (Total sum of squares) - ȳ is the mean of actual
 * values - Ranges from 0 to 1, where 1 indicates perfect fit The accumulator supports both
 * accumulation of new values and retraction of existing values, making it suitable for streaming
 * scenarios with updates and retractions.
 */
@Internal
public class RegressionEvaluatorAccumulator extends ModelEvaluatorAccumulator {
    private double sumActual = 0.0; // Sum of actual values (Σy_i)
    private double sumSquaredActual = 0.0; // Sum of squared actual values (Σy_i²)
    private double sumSquaredError = 0.0; // Sum of squared errors (Σ(y_i - ŷ_i)²)
    private double sumAbsoluteError = 0.0; // Sum of absolute errors (Σ|y_i - ŷ_i|)
    private double sumAbsPercError =
            0.0; // Sum of absolute percentage errors (Σ|((y_i - ŷ_i)/y_i)|)
    private int count = 0; // Number of samples (n)

    /**
     * Accumulates new values for regression metrics calculation.
     *
     * @param args Array of arguments in the following order: args[0] - actual value (double)
     *     args[1] - predicted value (double)
     * @throws IllegalArgumentException if the number of arguments is not exactly 2
     */
    @Override
    public void accumulate(Object... args) {
        if (args.length != 2) {
            throw new IllegalArgumentException(
                    "accumulate of RegressionEvaluatorAccumulator requires 2 arguments");
        }
        double actual = (double) args[0];
        double predicted = (double) args[1];
        // Update basic sums
        sumActual += actual;
        sumSquaredActual += actual * actual;
        // Calculate and accumulate errors
        double error = actual - predicted;
        sumSquaredError += error * error;
        sumAbsoluteError += Math.abs(error);
        // Calculate and accumulate percentage error if actual is not zero
        if (actual != 0.0) {
            sumAbsPercError += Math.abs(error / actual) * 100;
        }
        count++;
    }

    /**
     * Retracts previously accumulated values from the metrics calculation.
     *
     * @param args Array of arguments in the following order: args[0] - actual value to retract
     *     (double) args[1] - predicted value to retract (double)
     * @throws IllegalArgumentException if the number of arguments is not exactly 2
     */
    @Override
    public void retract(Object... args) {
        if (args.length != 2) {
            throw new IllegalArgumentException(
                    "retract of RegressionEvaluatorAccumulator requires 2 arguments");
        }
        double actual = (double) args[0];
        double predicted = (double) args[1];
        // Retract basic sums
        sumActual -= actual;
        sumSquaredActual -= actual * actual;
        // Retract errors
        double error = actual - predicted;
        sumSquaredError -= error * error;
        sumAbsoluteError -= Math.abs(error);
        // Retract percentage error if actual is not zero
        if (actual != 0.0) {
            sumAbsPercError -= Math.abs(error / actual) * 100;
        }
        count--;
    }

    /**
     * Returns the computed regression metrics as a Row containing calculated values. The metrics
     * are calculated as follows: MAE = sumAbsoluteError / count MSE = sumSquaredError / count RMSE
     * = √(MSE) MAPE = sumAbsPercError / count For R-squared: - Mean of actual values: ȳ = sumActual
     * / count - Total sum of squares: SStot = Σy_i² - (Σy_i)²/n - R² = 1 - (SSres/SStot) where
     * SSres is sumSquaredError
     *
     * @return A Row containing the metrics in order: MAE, MSE, RMSE, MAPE, R² Returns null if no
     *     samples have been accumulated (count = 0)
     */
    @Override
    public Row getValue() {
        if (count == 0) {
            return null;
        }
        double mae = sumAbsoluteError / count;
        double mse = sumSquaredError / count;
        double rmse = Math.sqrt(mse);
        double mape;
        if (count > 0 && sumAbsPercError > 0) {
            mape = sumAbsPercError / count;
        } else {
            mape = 0.0; // If all actual values are zero, MAPE is zero
        }
        double r2;
        double totalSS = sumSquaredActual - (sumActual * sumActual) / count;
        if (totalSS > 0) {
            r2 = 1.0 - (sumSquaredError / totalSS);
        } else {
            // If all actual values are identical, R² is undefined
            r2 = Double.NaN;
        }

        return Row.of(
                ImmutableMap.of(
                        "MAE", mae,
                        "MSE", mse,
                        "RMSE", rmse,
                        "MAPE", mape,
                        "R2", r2));
    }

    /**
     * Merges another RegressionEvaluatorAccumulator into this one. This is crucial for parallel
     * processing where partial results need to be combined. The merge operation performs
     * element-wise addition of all accumulated statistics: - Sums of actual values - Sums of
     * squared values - Error statistics - Sample counts Properties of merge operation: -
     * Commutative: merge(a,b) = merge(b,a) - Associative: merge(merge(a,b),c) = merge(a,merge(b,c))
     * - Identity: merge(a,empty) = a
     *
     * @param other The other accumulator to merge into this one
     * @throws IllegalArgumentException if other is not a RegressionEvaluatorAccumulator
     */
    @Override
    public void merge(ModelEvaluatorAccumulator other) {
        if (!(other instanceof RegressionEvaluatorAccumulator)) {
            throw new IllegalArgumentException("Can only merge RegressionEvaluatorAccumulator");
        }
        RegressionEvaluatorAccumulator otherAcc = (RegressionEvaluatorAccumulator) other;
        this.sumActual += otherAcc.sumActual;
        this.sumSquaredActual += otherAcc.sumSquaredActual;
        this.sumSquaredError += otherAcc.sumSquaredError;
        this.sumAbsoluteError += otherAcc.sumAbsoluteError;
        this.sumAbsPercError += otherAcc.sumAbsPercError;
        this.count += otherAcc.count;
    }

    /**
     * Resets the accumulator to its initial state. This operation: - Clears all accumulated
     * statistics - Resets sample counts to zero - Prepares accumulator for reuse After reset: -
     * getValue() will return null - Accumulator behaves as if newly created - All previous
     * accumulations are forgotten
     */
    @Override
    public void reset() {
        this.sumActual = 0.0;
        this.sumSquaredActual = 0.0;
        this.sumSquaredError = 0.0;
        this.sumAbsoluteError = 0.0;
        this.sumAbsPercError = 0.0;
        this.count = 0;
    }
}
