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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import java.io.Serializable;
import java.util.Map;

/**
 * Abstract base class for model evaluation accumulators in Flink ML functions. This class provides
 * the framework for accumulating and processing metrics during model evaluation operations.
 * Implementations of this class should handle specific model evaluation scenarios and maintain the
 * necessary state for metric calculations.
 */
public abstract class ModelEvaluatorAccumulator implements Serializable {
    /**
     * Accumulates input values for metric calculation.
     *
     * @param args Variable number of input arguments to be accumulated. These typically include
     *     predicted values, actual values, and any additional parameters needed for metric
     *     calculation.
     */
    public abstract void accumulate(Object... args);

    /**
     * Retracts previously accumulated values from the metrics calculation. This is particularly
     * useful in streaming scenarios where data may need to be removed from the accumulated state.
     *
     * @param args Variable number of input arguments to be retracted. These should match the format
     *     of arguments provided to accumulate().
     */
    public abstract void retract(Object... args);

    /**
     * Retrieves the current accumulated result as a Flink Row.
     *
     * @return A Row containing the calculated metric values based on all accumulated (and
     *     retracted) data points.
     */
    public abstract Map<String, Double> getValue();

    /**
     * Merges another accumulator into this one. This is crucial for parallel processing where
     * partial results need to be combined.
     *
     * @param other The other accumulator to merge into this one
     */
    public abstract void merge(ModelEvaluatorAccumulator other);

    /**
     * Resets the accumulator to its initial state. This is used when the accumulator needs to be
     * reused.
     */
    public abstract void reset();

    /**
     * Returns the output data type of the accumulator.
     *
     * @return The output data type of the accumulator as a Flink DataType
     */
    public static DataType getOutputType() {
        return DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.DOUBLE().notNull())
                .notNull()
                .bridgedTo(Map.class);
    }
}
