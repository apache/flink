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

import org.apache.flink.sql.parser.ddl.SqlAlterModelRename;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.ContextResolvedModel;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogModel;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.AlterModelRenameOperation;

import java.util.Optional;

/** A converter for {@link org.apache.flink.sql.parser.ddl.SqlAlterModelRename}. */
public class SqlAlterModelRenameConverter implements SqlNodeConverter<SqlAlterModelRename> {

    @Override
    public Operation convertSqlNode(
            SqlAlterModelRename sqlAlterModelRename, ConvertContext context) {
        final CatalogManager catalogManager = context.getCatalogManager();
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlAlterModelRename.fullModelName());
        ObjectIdentifier modelIdentifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);
        Optional<ContextResolvedModel> optionalCatalogModel =
                catalogManager.getModel(modelIdentifier);
        if (optionalCatalogModel.isEmpty() || optionalCatalogModel.get().isTemporary()) {
            if (optionalCatalogModel.isEmpty()) {
                if (!sqlAlterModelRename.ifModelExists()) {
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

        // Rename model
        UnresolvedIdentifier newUnresolvedIdentifier =
                UnresolvedIdentifier.of(sqlAlterModelRename.fullNewModelName());
        ObjectIdentifier newModelIdentifier =
                catalogManager.qualifyIdentifier(newUnresolvedIdentifier);
        return new AlterModelRenameOperation(
                existingModel,
                modelIdentifier,
                newModelIdentifier,
                sqlAlterModelRename.ifModelExists());
    }
}
