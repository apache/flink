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

package org.apache.flink.table.operations.ddl;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.TableResultImpl;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.CatalogChange;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.utils.EncodingUtils;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Operation to describe a ALTER CATALOG COMMENT statement. */
@Internal
public class AlterCatalogCommentOperation implements AlterOperation {

    private final String catalogName;
    private final String comment;

    public AlterCatalogCommentOperation(String catalogName, String comment) {
        this.catalogName = checkNotNull(catalogName);
        this.comment = comment;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getComment() {
        return comment;
    }

    @Override
    public String asSummaryString() {
        return String.format(
                "ALTER CATALOG %s COMMENT '%s'",
                catalogName, EncodingUtils.escapeSingleQuotes(comment));
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        try {
            ctx.getCatalogManager()
                    .alterCatalog(catalogName, new CatalogChange.CatalogCommentChange(comment));

            return TableResultImpl.TABLE_RESULT_OK;
        } catch (CatalogException e) {
            throw new ValidationException(
                    String.format("Could not execute %s", asSummaryString()), e);
        }
    }
}
