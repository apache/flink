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

import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.CreateViewOperation;
import org.apache.flink.table.planner.utils.OperationConverterUtils;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.NlsString;

import java.util.List;
import java.util.Map;

/** A converter for {@link SqlCreateView}. */
public class SqlCreateViewConverter implements SqlNodeConverter<SqlCreateView> {

    @Override
    public Operation convertSqlNode(SqlCreateView sqlCreateView, ConvertContext context) {
        final SqlNode query = sqlCreateView.getQuery();
        final List<SqlNode> viewFields = sqlCreateView.getFieldList().getList();

        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlCreateView.fullViewName());
        ObjectIdentifier identifier =
                context.getCatalogManager().qualifyIdentifier(unresolvedIdentifier);

        String viewComment =
                sqlCreateView
                        .getComment()
                        .map(c -> c.getValueAs(NlsString.class).getValue())
                        .orElse(null);
        Map<String, String> viewOptions =
                OperationConverterUtils.extractProperties(
                        sqlCreateView.getProperties().orElse(null));
        CatalogView catalogView =
                SqlNodeConvertUtils.toCatalogView(
                        query, viewFields, viewOptions, viewComment, context);
        return new CreateViewOperation(
                identifier,
                catalogView,
                sqlCreateView.isIfNotExists(),
                sqlCreateView.isTemporary());
    }
}
