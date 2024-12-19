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

import org.apache.flink.sql.parser.ddl.SqlAlterModel;
import org.apache.flink.sql.parser.ddl.SqlAlterModelRename;
import org.apache.flink.sql.parser.ddl.SqlAlterModelReset;
import org.apache.flink.sql.parser.ddl.SqlAlterModelSet;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.ContextResolvedModel;
import org.apache.flink.table.catalog.ModelChange;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogModel;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.AlterModelChangeOperation;
import org.apache.flink.table.operations.ddl.AlterModelRenameOperation;
import org.apache.flink.table.planner.utils.OperationConverterUtils;

import org.apache.calcite.sql.dialect.CalciteSqlDialect;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** Helper class for converting {@link SqlAlterModel} to {@link AlterModelChangeOperation}. */
public class SqlAlterModelConverter {
    private final CatalogManager catalogManager;

    SqlAlterModelConverter(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
    }

    public Operation convertAlterModel(SqlAlterModel sqlAlterModel) {
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlAlterModel.fullModelName());
        ObjectIdentifier modelIdentifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);
        Optional<ContextResolvedModel> optionalCatalogModel =
                catalogManager.getModel(modelIdentifier);
        if (optionalCatalogModel.isEmpty() || optionalCatalogModel.get().isTemporary()) {
            if (optionalCatalogModel.isEmpty()) {
                if (!sqlAlterModel.ifModelExists()) {
                    throw new ValidationException(
                            String.format("Model %s doesn't exist.", modelIdentifier));
                }
            } else if (optionalCatalogModel.get().isTemporary()) {
                throw new ValidationException(
                        String.format("Model %s is a temporary model.", modelIdentifier));
            }
        }
        ResolvedCatalogModel existingModel =
                optionalCatalogModel.map(ContextResolvedModel::getResolvedModel).orElse(null);

        if (sqlAlterModel instanceof SqlAlterModelRename) {
            SqlAlterModelRename sqlAlterModelRename = (SqlAlterModelRename) sqlAlterModel;
            // Rename model
            UnresolvedIdentifier newUnresolvedIdentifier =
                    UnresolvedIdentifier.of(sqlAlterModelRename.fullNewModelName());
            ObjectIdentifier newModelIdentifier =
                    catalogManager.qualifyIdentifier(newUnresolvedIdentifier);
            return new AlterModelRenameOperation(
                    existingModel,
                    modelIdentifier,
                    newModelIdentifier,
                    sqlAlterModel.ifModelExists());
        } else if (sqlAlterModel instanceof SqlAlterModelSet) {
            return convertAlterModelSet(
                    modelIdentifier, (SqlAlterModelSet) sqlAlterModel, existingModel);
        } else if (sqlAlterModel instanceof SqlAlterModelReset) {
            return convertAlterModelReset(
                    modelIdentifier, (SqlAlterModelReset) sqlAlterModel, existingModel);
        } else {
            throw new ValidationException(
                    String.format(
                            "[%s] needs to implement",
                            sqlAlterModel.toSqlString(CalciteSqlDialect.DEFAULT)));
        }
    }

    private static AlterModelChangeOperation convertAlterModelSet(
            ObjectIdentifier modelIdentifier,
            SqlAlterModelSet sqlAlterModelSet,
            @Nullable ResolvedCatalogModel existingModel) {
        Map<String, String> changeModelOptions =
                OperationConverterUtils.extractProperties(sqlAlterModelSet.getOptionList());
        if (changeModelOptions.isEmpty()) {
            throw new ValidationException("ALTER MODEL SET does not support empty option");
        }
        List<ModelChange> modelChanges = new ArrayList<>();
        changeModelOptions.forEach((key, value) -> modelChanges.add(ModelChange.set(key, value)));

        if (existingModel == null) {
            return new AlterModelChangeOperation(
                    modelIdentifier, modelChanges, null, sqlAlterModelSet.ifModelExists());
        }

        Map<String, String> newOptions =
                existingModel.getOptions().entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        entry -> entry.getKey().toLowerCase(), Entry::getValue));
        Map<String, String> lowercaseChangeModelOptions =
                changeModelOptions.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        entry -> entry.getKey().toLowerCase(), Entry::getValue));
        newOptions.putAll(lowercaseChangeModelOptions);

        return new AlterModelChangeOperation(
                modelIdentifier,
                modelChanges,
                existingModel.copy(newOptions),
                sqlAlterModelSet.ifModelExists());
    }

    private Operation convertAlterModelReset(
            ObjectIdentifier modelIdentifier,
            SqlAlterModelReset sqlAlterModelReset,
            @Nullable ResolvedCatalogModel oldModel) {
        Set<String> lowercaseResetKeys =
                sqlAlterModelReset.getResetKeys().stream()
                        .map(String::toLowerCase)
                        .collect(Collectors.toSet());
        if (lowercaseResetKeys.isEmpty()) {
            throw new ValidationException("ALTER MODEL RESET does not support empty key");
        }
        List<ModelChange> modelChanges =
                lowercaseResetKeys.stream().map(ModelChange::reset).collect(Collectors.toList());

        if (oldModel == null) {
            return new AlterModelChangeOperation(
                    modelIdentifier, modelChanges, null, sqlAlterModelReset.ifModelExists());
        }

        Map<String, String> newOptions =
                oldModel.getOptions().entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        entry -> entry.getKey().toLowerCase(), Entry::getValue));
        // reset table option keys
        lowercaseResetKeys.forEach(newOptions::remove);
        return new AlterModelChangeOperation(
                modelIdentifier,
                modelChanges,
                oldModel.copy(newOptions),
                sqlAlterModelReset.ifModelExists());
    }
}
