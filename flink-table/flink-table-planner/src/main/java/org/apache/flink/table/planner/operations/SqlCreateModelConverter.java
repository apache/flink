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

package org.apache.flink.table.planner.operations;

import org.apache.flink.sql.parser.ddl.SqlCreateModel;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlRegularColumn;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogModel;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.CreateModelOperation;
import org.apache.flink.table.planner.calcite.FlinkCalciteSqlValidator;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.utils.OperationConverterUtils;
import org.apache.flink.table.types.DataType;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

/** Helper class for converting {@link SqlCreateModel} to {@link CreateModelOperation}. */
public class SqlCreateModelConverter {
    private final CatalogManager catalogManager;
    private final FlinkCalciteSqlValidator sqlValidator;

    SqlCreateModelConverter(FlinkCalciteSqlValidator sqlValidator, CatalogManager catalogManager) {
        this.sqlValidator = sqlValidator;
        this.catalogManager = catalogManager;
    }

    /** Convert the {@link SqlCreateModel} node. */
    public Operation convertCreateModel(SqlCreateModel sqlCreateModel) {
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlCreateModel.fullModelName());
        ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);
        Map<String, String> modelOptions = getModelOptions(sqlCreateModel);
        CatalogModel catalogModel =
                CatalogModel.of(
                        getSchema(sqlCreateModel.getInputColumnList()),
                        getSchema(sqlCreateModel.getOutputColumnList()),
                        modelOptions,
                        sqlCreateModel.getComment().map(SqlLiteral::toValue).orElse(null));

        return new CreateModelOperation(
                identifier,
                catalogManager.resolveCatalogModel(catalogModel),
                sqlCreateModel.isIfNotExists(),
                sqlCreateModel.isTemporary());
    }

    private Schema getSchema(SqlNodeList nodeList) {
        final List<Schema.UnresolvedColumn> columnList = new ArrayList<>();
        for (SqlNode column : nodeList) {
            if (column instanceof SqlRegularColumn) {
                SqlRegularColumn regularColumn = (SqlRegularColumn) column;
                SqlDataTypeSpec type = regularColumn.getType();
                boolean nullable = type.getNullable() == null || type.getNullable();
                RelDataType relType = type.deriveType(sqlValidator, nullable);
                DataType dataType = fromLogicalToDataType(FlinkTypeFactory.toLogicalType(relType));
                columnList.add(
                        new Schema.UnresolvedPhysicalColumn(
                                regularColumn.getName().getSimple(),
                                dataType,
                                OperationConverterUtils.getComment(regularColumn)));
            } else {
                throw new TableException("Column " + column + " can only be SqlRegularColumn");
            }
        }
        final Schema.Builder builder = Schema.newBuilder();
        builder.fromColumns(columnList);
        return builder.build();
    }

    private Map<String, String> getModelOptions(SqlCreateModel sqlCreateModel) {
        Map<String, String> options = new HashMap<>();
        sqlCreateModel
                .getPropertyList()
                .getList()
                .forEach(
                        p ->
                                options.put(
                                        ((SqlTableOption) Objects.requireNonNull(p))
                                                .getKeyString()
                                                .toUpperCase(),
                                        ((SqlTableOption) p).getValueString()));
        return options;
    }
}
