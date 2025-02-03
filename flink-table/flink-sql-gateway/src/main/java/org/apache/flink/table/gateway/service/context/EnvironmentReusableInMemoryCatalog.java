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

package org.apache.flink.table.gateway.service.context;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.QueryOperationCatalogView;
import org.apache.flink.table.catalog.ResolvedCatalogView;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;

import java.util.Optional;

/**
 * An in-memory catalog that can be reused across different {@link TableEnvironment}. The SQL client
 * works against {@link TableEnvironment} design and reuses some of the components (e.g.
 * CatalogManager), but not all (e.g. Planner) which causes e.g. views registered in an in-memory
 * catalog to fail. This class is a workaround not to keep Planner bound parts of a view reused
 * across different {@link TableEnvironment}.
 */
public class EnvironmentReusableInMemoryCatalog extends GenericInMemoryCatalog {
    public EnvironmentReusableInMemoryCatalog(String name, String defaultDatabase) {
        super(name, defaultDatabase);
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException {
        CatalogBaseTable tableToRegister =
                extractView(table)
                        .flatMap(QueryOperationCatalogView::getOriginalView)
                        .map(v -> (CatalogBaseTable) v)
                        .orElse(table);
        super.createTable(tablePath, tableToRegister, ignoreIfExists);
    }

    private Optional<QueryOperationCatalogView> extractView(CatalogBaseTable table) {
        if (table instanceof ResolvedCatalogView) {
            final CatalogView origin = ((ResolvedCatalogView) table).getOrigin();
            if (origin instanceof QueryOperationCatalogView) {
                return Optional.of((QueryOperationCatalogView) origin);
            }
            return Optional.empty();
        } else if (table instanceof QueryOperationCatalogView) {
            return Optional.of((QueryOperationCatalogView) table);
        }
        return Optional.empty();
    }
}
