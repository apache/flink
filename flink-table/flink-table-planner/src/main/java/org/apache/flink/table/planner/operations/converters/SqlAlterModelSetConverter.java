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

import org.apache.flink.sql.parser.ddl.SqlAlterModelSet;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ModelChange;
import org.apache.flink.table.catalog.ResolvedCatalogModel;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.AlterModelChangeOperation;
import org.apache.flink.table.planner.utils.OperationConverterUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A converter for {@link org.apache.flink.sql.parser.ddl.SqlAlterModelSet}. */
public class SqlAlterModelSetConverter extends AbstractSqlAlterModelConverter<SqlAlterModelSet> {

    @Override
    public Operation convertSqlNode(SqlAlterModelSet sqlAlterModelSet, ConvertContext context) {
        ResolvedCatalogModel existingModel =
                getExistingModel(
                        context,
                        sqlAlterModelSet.fullModelName(),
                        sqlAlterModelSet.ifModelExists());

        Map<String, String> changeModelOptions =
                OperationConverterUtils.getTableOptions(sqlAlterModelSet.getOptionList());
        if (changeModelOptions.isEmpty()) {
            throw new ValidationException("ALTER MODEL SET does not support empty option.");
        }
        List<ModelChange> modelChanges = new ArrayList<>();
        changeModelOptions.forEach((key, value) -> modelChanges.add(ModelChange.set(key, value)));

        if (existingModel == null) {
            return new AlterModelChangeOperation(
                    context.getCatalogManager()
                            .qualifyIdentifier(
                                    UnresolvedIdentifier.of(sqlAlterModelSet.fullModelName())),
                    modelChanges,
                    null,
                    sqlAlterModelSet.ifModelExists());
        }

        Map<String, String> newOptions = new HashMap<>(existingModel.getOptions());
        newOptions.putAll(changeModelOptions);

        return new AlterModelChangeOperation(
                context.getCatalogManager()
                        .qualifyIdentifier(
                                UnresolvedIdentifier.of(sqlAlterModelSet.fullModelName())),
                modelChanges,
                existingModel.copy(newOptions),
                sqlAlterModelSet.ifModelExists());
    }
}
