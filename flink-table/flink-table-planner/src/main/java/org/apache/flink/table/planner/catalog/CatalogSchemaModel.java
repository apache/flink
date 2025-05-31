/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.catalog;

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ContextResolvedModel;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.ModelProviderFactory;
import org.apache.flink.table.ml.ModelProvider;
import org.apache.flink.table.module.Module;
import org.apache.flink.table.planner.calcite.FlinkCalciteSqlValidator;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.RexModelCall;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.runtime.types.PlannerTypeUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql2rel.SqlRexContext;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.util.OptionalUtils.firstPresent;

/**
 * Represents a wrapper for {@link ContextResolvedModel} in {@link
 * org.apache.calcite.schema.Schema}.
 */
public class CatalogSchemaModel {
    // ~ Instance fields --------------------------------------------------------

    private final ContextResolvedModel contextResolvedModel;

    // ~ Constructors -----------------------------------------------------------

    /**
     * Create a CatalogSchemaModel instance.
     *
     * @param contextResolvedModel A result of catalog lookup
     */
    public CatalogSchemaModel(ContextResolvedModel contextResolvedModel) {
        this.contextResolvedModel = contextResolvedModel;
    }

    // ~ Methods ----------------------------------------------------------------

    public ContextResolvedModel getContextResolvedModel() {
        return contextResolvedModel;
    }

    public boolean isTemporary() {
        return contextResolvedModel.isTemporary();
    }

    public RelDataType getInputRowType(RelDataTypeFactory typeFactory) {
        final FlinkTypeFactory flinkTypeFactory = (FlinkTypeFactory) typeFactory;
        final ResolvedSchema schema =
                contextResolvedModel.getResolvedModel().getResolvedInputSchema();
        return schemaToRelDataType(flinkTypeFactory, schema);
    }

    public RelDataType getOutputRowType(RelDataTypeFactory typeFactory) {
        final FlinkTypeFactory flinkTypeFactory = (FlinkTypeFactory) typeFactory;
        final ResolvedSchema schema =
                contextResolvedModel.getResolvedModel().getResolvedOutputSchema();
        return schemaToRelDataType(flinkTypeFactory, schema);
    }

    public RexNode toRex(SqlRexContext rexContext) {
        FlinkCalciteSqlValidator validator = (FlinkCalciteSqlValidator) rexContext.getValidator();
        RelOptCluster cluster = validator.getRelOptCluster();
        FlinkContext context = ShortcutUtils.unwrapContext(cluster);
        ModelProvider modelProvider = createModelProvider(context, contextResolvedModel);
        return new RexModelCall(
                getInputRowType(validator.getTypeFactory()), contextResolvedModel, modelProvider);
    }

    private static RelDataType schemaToRelDataType(
            FlinkTypeFactory typeFactory, ResolvedSchema schema) {
        final List<String> fieldNames = schema.getColumnNames();
        final List<LogicalType> fieldTypes =
                schema.getColumnDataTypes().stream()
                        .map(DataType::getLogicalType)
                        .map(PlannerTypeUtils::removeLegacyTypes)
                        .collect(Collectors.toList());
        return typeFactory.buildRelNodeRowType(fieldNames, fieldTypes);
    }

    private ModelProvider createModelProvider(
            FlinkContext context, ContextResolvedModel catalogModel) {

        final Optional<ModelProviderFactory> factoryFromCatalog =
                catalogModel
                        .getCatalog()
                        .flatMap(Catalog::getFactory)
                        .map(
                                f ->
                                        f instanceof ModelProviderFactory
                                                ? (ModelProviderFactory) f
                                                : null);

        final Optional<ModelProviderFactory> factoryFromModule =
                context.getModuleManager().getFactory(Module::getModelProviderFactory);

        // Since the catalog is more specific, we give it precedence over a factory provided by any
        // modules.
        final ModelProviderFactory factory =
                firstPresent(factoryFromCatalog, factoryFromModule).orElse(null);

        return FactoryUtil.createModelProvider(
                factory,
                contextResolvedModel.getIdentifier(),
                contextResolvedModel.getResolvedModel(),
                context.getTableConfig(),
                context.getClassLoader(),
                contextResolvedModel.isTemporary());
    }
}
