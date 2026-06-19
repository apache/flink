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

package org.apache.flink.table.api.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ApiExpression;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Model;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.ContextResolvedModel;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.expressions.ApiExpressionUtils;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.types.ColumnList;

import java.util.ArrayList;
import java.util.Map;

import static org.apache.flink.table.api.Expressions.lit;
import static org.apache.flink.table.expressions.ApiExpressionUtils.valueLiteral;

/** Implementation of {@link Model} that works with the Table API. */
@Internal
public class ModelImpl implements Model {

    private final TableEnvironmentInternal tableEnvironment;
    private final ContextResolvedModel model;

    private ModelImpl(TableEnvironmentInternal tableEnvironment, ContextResolvedModel model) {
        this.tableEnvironment = tableEnvironment;
        this.model = model;
    }

    public static ModelImpl createModel(
            TableEnvironmentInternal tableEnvironment, ContextResolvedModel model) {
        return new ModelImpl(tableEnvironment, model);
    }

    public ContextResolvedModel getModel() {
        return model;
    }

    @Override
    public ResolvedSchema getResolvedInputSchema() {
        return model.getResolvedModel().getResolvedInputSchema();
    }

    @Override
    public ResolvedSchema getResolvedOutputSchema() {
        return model.getResolvedModel().getResolvedOutputSchema();
    }

    public TableEnvironment getTableEnv() {
        return tableEnvironment;
    }

    @Override
    public Table predict(Table table, ColumnList inputColumns) {
        return predict(table, inputColumns, Map.of());
    }

    @Override
    public Table predict(Table table, ColumnList inputColumns, Map<String, String> options) {
        // Use Expressions.map() instead of Expressions.lit() to create a MAP literal since
        // lit() is not serializable to sql.
        if (options.isEmpty()) {
            return tableEnvironment.fromCall(
                    BuiltInFunctionDefinitions.ML_PREDICT.getName(),
                    table.asArgument("INPUT"),
                    this.asArgument("MODEL"),
                    new ApiExpression(valueLiteral(inputColumns)).asArgument("ARGS"));
        }
        ArrayList<String> configKVs = new ArrayList<>();
        options.forEach(
                (k, v) -> {
                    configKVs.add(k);
                    configKVs.add(v);
                });
        return tableEnvironment.fromCall(
                BuiltInFunctionDefinitions.ML_PREDICT.getName(),
                table.asArgument("INPUT"),
                this.asArgument("MODEL"),
                new ApiExpression(valueLiteral(inputColumns)).asArgument("ARGS"),
                Expressions.map(
                                configKVs.get(0),
                                configKVs.get(1),
                                configKVs.subList(2, configKVs.size()).toArray())
                        .asArgument("CONFIG"));
    }

    @Override
    public ApiExpression asArgument(String name) {
        return new ApiExpression(
                ApiExpressionUtils.unresolvedCall(
                        BuiltInFunctionDefinitions.ASSIGNMENT,
                        lit(name),
                        ApiExpressionUtils.modelRef(name, this)));
    }

    public TableEnvironment getTableEnvironment() {
        return tableEnvironment;
    }
}
