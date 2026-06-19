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
 * Operation to describe a SHOW CATALOGS statement. The full syntax for SHOW CATALOGS is as
 * followings:
 *
 * <pre>{@code
 * SHOW CATALOGS [ [NOT] (LIKE | ILIKE) <sql_like_pattern> ]
 * }</pre>
 */
@Internal
public class ShowCatalogsOperation extends AbstractShowOperation {

    public ShowCatalogsOperation(@Nullable ShowLikeOperator likeOp) {
        super(null, null, likeOp);
    }

    @Override
    protected String getOperationName() {
        return "SHOW CATALOGS";
    }

    @Override
    protected String getColumnName() {
        return "catalog name";
    }

    @Override
    protected Collection<String> retrieveDataForTableResult(Context ctx) {
        final CatalogManager catalogManager = ctx.getCatalogManager();
        return catalogManager.listCatalogs();
    }
}
