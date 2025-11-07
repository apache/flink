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

package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.config.MLPredictRuntimeConfigOptions;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.types.ColumnList;

import java.util.Map;

/**
 * The {@link Model} object is the core abstraction for ML model resources in the Table API.
 *
 * <p>A {@link Model} object describes a machine learning model resource that can be used for
 * inference operations. It provides methods to perform prediction on data tables.
 *
 * <p>The {@link Model} interface offers main operations:
 *
 * <ul>
 *   <li>{@link #predict(Table, ColumnList)} - Applies the model to make predictions on input data
 * </ul>
 *
 * <p>{@code ml_predict} operation supports runtime options for configuring execution parameters
 * such as asynchronous execution mode.
 *
 * <p>Every {@link Model} object has input and output schemas that describe the expected data
 * structure for model operations, available through {@link #getResolvedInputSchema()} and {@link
 * #getResolvedOutputSchema()}.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * Model model = tableEnv.fromModel("my_model");
 *
 * // Simple prediction
 * Table predictions = model.predict(inputTable, ColumnList.of("feature1", "feature2"));
 *
 * // Prediction with options
 * Map<String, String> options = Map.of("max-concurrent-operations", "100", "timeout", "30s", "async", "true");
 * Table predictions = model.predict(inputTable, ColumnList.of("feature1", "feature2"), options);
 * }</pre>
 */
@PublicEvolving
public interface Model {

    /**
     * Returns the resolved input schema of this model.
     *
     * <p>The input schema describes the structure and data types of the input columns that the
     * model expects for inference operations.
     *
     * @return the resolved input schema.
     */
    ResolvedSchema getResolvedInputSchema();

    /**
     * Returns the resolved output schema of this model.
     *
     * <p>The output schema describes the structure and data types of the output columns that the
     * model produces during inference operations.
     *
     * @return the resolved output schema.
     */
    ResolvedSchema getResolvedOutputSchema();

    /**
     * Performs prediction on the given table using specified input columns.
     *
     * <p>This method applies the model to the input data to generate predictions. The input columns
     * must match the model's expected input schema.
     *
     * <p>Example:
     *
     * <pre>{@code
     * Table predictions = model.predict(inputTable, ColumnList.of("feature1", "feature2"));
     * }</pre>
     *
     * @param table the input table containing data for prediction
     * @param inputColumns the columns from the input table to use as model input
     * @return a table containing the input data along with prediction results
     */
    Table predict(Table table, ColumnList inputColumns);

    /**
     * Performs prediction on the given table using specified input columns with runtime options.
     *
     * <p>This method applies the model to the input data to generate predictions with additional
     * runtime configuration options such as max-concurrent-operations, timeout, and execution mode
     * settings.
     *
     * <p>For Common runtime options, see {@link MLPredictRuntimeConfigOptions}.
     *
     * <p>Example:
     *
     * <pre>{@code
     * Map<String, String> options = Map.of("max-concurrent-operations", "100", "timeout", "30s", "async", "true");
     * Table predictions = model.predict(inputTable,
     *     ColumnList.of("feature1", "feature2"), options);
     * }</pre>
     *
     * @param table the input table containing data for prediction
     * @param inputColumns the columns from the input table to use as model input
     * @param options runtime options for configuring the prediction operation
     * @return a table containing the input data along with prediction results
     */
    Table predict(Table table, ColumnList inputColumns, Map<String, String> options);

    /**
     * Converts this model object into a named argument.
     *
     * <p>This method is intended for use in function calls that accept model arguments,
     * particularly in process table functions (PTFs) or other operations that work with models.
     *
     * <p>Example:
     *
     * <pre>{@code
     * env.fromCall(
     *   "ML_PREDICT",
     *   inputTable.asArgument("INPUT"),
     *   model.asArgument("MODEL"),
     *   Expressions.descriptor(ColumnList.of("feature1", "feature2")).asArgument("ARGS")
     * )
     * }</pre>
     *
     * @param name the name to assign to this model argument
     * @return an expression that can be passed to functions expecting model arguments
     */
    ApiExpression asArgument(String name);
}
