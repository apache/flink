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
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.AlterModelRenameOperation;

/** A converter for {@link org.apache.flink.sql.parser.ddl.SqlAlterModelRename}. */
public class SqlAlterModelRenameConverter
        extends AbstractSqlAlterModelConverter<SqlAlterModelRename> {

    @Override
    public Operation convertSqlNode(
            SqlAlterModelRename sqlAlterModelRename, ConvertContext context) {
        // Mainly to check model existence
        getExistingModel(
                context, sqlAlterModelRename.fullModelName(), sqlAlterModelRename.ifModelExists());

        UnresolvedIdentifier newUnresolvedIdentifier =
                UnresolvedIdentifier.of(sqlAlterModelRename.fullNewModelName());
        ObjectIdentifier newModelIdentifier =
                context.getCatalogManager().qualifyIdentifier(newUnresolvedIdentifier);
        ObjectIdentifier oldModelIdentifier =
                context.getCatalogManager()
                        .qualifyIdentifier(
                                UnresolvedIdentifier.of(sqlAlterModelRename.fullModelName()));

        if (!newModelIdentifier.getCatalogName().equals(oldModelIdentifier.getCatalogName())) {
            throw new ValidationException(
                    String.format(
                            "The catalog name of the new model name '%s' must be the same as the old model name '%s'.",
                            newModelIdentifier.asSummaryString(),
                            oldModelIdentifier.asSummaryString()));
        }

        return new AlterModelRenameOperation(
                oldModelIdentifier, newModelIdentifier, sqlAlterModelRename.ifModelExists());
    }
}
