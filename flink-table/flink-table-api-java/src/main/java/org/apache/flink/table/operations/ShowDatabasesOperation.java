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

package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.operations.utils.ShowLikeOperator;

import javax.annotation.Nullable;

import java.util.Collection;

/**
 * Operation to describe a SHOW DATABASES statement. The full syntax for SHOW DATABASES is as
 * followings:
 *
 * <pre>{@code
 * SHOW DATABASES [ ( FROM | IN ) catalog_name] [ [NOT] (LIKE | ILIKE) &lt;sql_like_pattern&gt; ]
 * }</pre>
 */
@Internal
public class ShowDatabasesOperation extends AbstractShowOperation {

    public ShowDatabasesOperation(
            @Nullable String catalogName,
            @Nullable String preposition,
            @Nullable ShowLikeOperator likeOp) {
        super(catalogName, preposition, likeOp);
    }

    public ShowDatabasesOperation(@Nullable String catalogName, @Nullable ShowLikeOperator likeOp) {
        this(catalogName, null, likeOp);
    }

    public ShowDatabasesOperation(@Nullable String catalogName) {
        this(catalogName, null, null);
    }

    @Override
    protected Collection<String> retrieveDataForTableResult(Context ctx) {
        final CatalogManager catalogManager = ctx.getCatalogManager();
        final String qualifiedCatalogName = catalogManager.qualifyCatalog(catalogName);
        return catalogManager.getCatalogOrThrowException(qualifiedCatalogName).listDatabases();
    }

    @Override
    protected String getOperationName() {
        return "SHOW DATABASES";
    }

    @Override
    protected String getColumnName() {
        return "database name";
    }
}
