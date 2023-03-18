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

import org.apache.flink.sql.parser.ddl.SqlAlterViewProperties;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.AlterViewPropertiesOperation;
import org.apache.flink.table.planner.utils.OperationConverterUtils;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.planner.operations.converters.SqlNodeConvertUtils.validateAlterView;

/** A converter for {@link SqlAlterViewProperties}. */
public class SqlAlterViewPropertiesConverter implements SqlNodeConverter<SqlAlterViewProperties> {

    @Override
    public Operation convertSqlNode(SqlAlterViewProperties alterView, ConvertContext context) {
        CatalogView oldView = validateAlterView(alterView, context);
        ObjectIdentifier viewIdentifier =
                context.getCatalogManager()
                        .qualifyIdentifier(UnresolvedIdentifier.of(alterView.fullViewName()));
        Map<String, String> newOptions = new HashMap<>(oldView.getOptions());
        newOptions.putAll(OperationConverterUtils.extractProperties(alterView.getPropertyList()));
        CatalogView newView =
                CatalogView.of(
                        oldView.getUnresolvedSchema(),
                        oldView.getComment(),
                        oldView.getOriginalQuery(),
                        oldView.getExpandedQuery(),
                        newOptions);
        return new AlterViewPropertiesOperation(viewIdentifier, newView);
    }
}
