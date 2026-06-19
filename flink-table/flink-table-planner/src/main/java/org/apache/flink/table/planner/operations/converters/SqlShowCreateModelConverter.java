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

import org.apache.flink.sql.parser.dql.SqlShowCreateModel;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ContextResolvedModel;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ShowCreateModelOperation;

import java.util.Optional;

/** A converter for {@link org.apache.flink.sql.parser.dql.SqlShowCreateModel}. */
public class SqlShowCreateModelConverter implements SqlNodeConverter<SqlShowCreateModel> {

    @Override
    public Operation convertSqlNode(SqlShowCreateModel showCreateModel, ConvertContext context) {
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(showCreateModel.getFullModelName());
        ObjectIdentifier identifier =
                context.getCatalogManager().qualifyIdentifier(unresolvedIdentifier);
        Optional<ContextResolvedModel> model = context.getCatalogManager().getModel(identifier);

        if (model.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Could not execute SHOW CREATE MODEL. Model with identifier %s does not exist.",
                            identifier.asSerializableString()));
        }

        return new ShowCreateModelOperation(
                identifier, model.get().getResolvedModel(), model.get().isTemporary());
    }
}
