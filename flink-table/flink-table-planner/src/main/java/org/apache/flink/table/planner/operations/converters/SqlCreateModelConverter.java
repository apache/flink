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

package org.apache.flink.table.planner.operations.converters;

import org.apache.flink.sql.parser.ddl.SqlCreateModel;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlRegularColumn;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogModel;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.CreateModelOperation;

import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.validate.SqlValidator;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/** A converter for {@link org.apache.flink.sql.parser.ddl.SqlCreateModel}. */
public class SqlCreateModelConverter implements SqlNodeConverter<SqlCreateModel> {

    @Override
    public Operation convertSqlNode(SqlCreateModel sqlCreateModel, ConvertContext context) {
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlCreateModel.fullModelName());
        ObjectIdentifier identifier =
                context.getCatalogManager().qualifyIdentifier(unresolvedIdentifier);
        Map<String, String> modelOptions = getModelOptions(sqlCreateModel);

        ModelSchemaBuilderUtils schemaBuilderUtil =
                new ModelSchemaBuilderUtils(
                        context.getSqlValidator(),
                        SqlNode::toString,
                        context.getCatalogManager().getDataTypeFactory());

        CatalogModel catalogModel =
                CatalogModel.of(
                        schemaBuilderUtil.getSchema(sqlCreateModel.getInputColumnList(), true),
                        schemaBuilderUtil.getSchema(sqlCreateModel.getOutputColumnList(), false),
                        modelOptions,
                        sqlCreateModel.getComment().map(SqlLiteral::toValue).orElse(null));

        return new CreateModelOperation(
                identifier,
                context.getCatalogManager().resolveCatalogModel(catalogModel),
                sqlCreateModel.isIfNotExists(),
                sqlCreateModel.isTemporary());
    }

    private Map<String, String> getModelOptions(SqlCreateModel sqlCreateModel) {
        Map<String, String> options = new HashMap<>();
        sqlCreateModel
                .getPropertyList()
                .getList()
                .forEach(
                        p ->
                                options.put(
                                        ((SqlTableOption) Objects.requireNonNull(p)).getKeyString(),
                                        ((SqlTableOption) p).getValueString()));
        return options;
    }

    /** Builder for {@link Schema} of a model. */
    private static class ModelSchemaBuilderUtils extends SchemaBuilderUtil {
        ModelSchemaBuilderUtils(
                SqlValidator sqlValidator,
                Function<SqlNode, String> escapeExpressions,
                DataTypeFactory dataTypeFactory) {
            super(sqlValidator, escapeExpressions, dataTypeFactory);
        }

        private Schema getSchema(SqlNodeList nodeList, boolean isInput) {
            columns.clear();
            String schemaType = isInput ? "input" : "output";
            for (SqlNode column : nodeList) {
                if (column instanceof SqlRegularColumn) {
                    SqlRegularColumn regularColumn = (SqlRegularColumn) column;
                    String name = regularColumn.getName().getSimple();
                    if (columns.containsKey(name)) {
                        throw new ValidationException(
                                "Duplicate " + schemaType + " column name: '" + name + "'.");
                    }

                    columns.put(name, toUnresolvedPhysicalColumn(regularColumn));
                } else {
                    throw new ValidationException(
                            "Column " + column + " can only be a physical column.");
                }
            }
            return build();
        }
    }
}
