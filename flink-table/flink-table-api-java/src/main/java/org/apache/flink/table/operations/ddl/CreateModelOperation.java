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
import org.apache.flink.table.api.internal.TableResultImpl;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.CatalogModel;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogModel;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.OperationUtils;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Operation to describe a CREATE MODEL statement. */
@Internal
public class CreateModelOperation implements CreateOperation {

    private final ObjectIdentifier modelIdentifier;
    private final ResolvedCatalogModel catalogModel;
    private final boolean ignoreIfExists;
    private final boolean isTemporary;

    public CreateModelOperation(
            ObjectIdentifier modelIdentifier,
            ResolvedCatalogModel catalogModel,
            boolean ignoreIfExists,
            boolean isTemporary) {
        this.modelIdentifier = modelIdentifier;
        this.catalogModel = catalogModel;
        this.ignoreIfExists = ignoreIfExists;
        this.isTemporary = isTemporary;
    }

    public CatalogModel getCatalogModel() {
        return catalogModel;
    }

    public ObjectIdentifier getModelIdentifier() {
        return modelIdentifier;
    }

    public boolean isIgnoreIfExists() {
        return ignoreIfExists;
    }

    public boolean isTemporary() {
        return isTemporary;
    }

    @Override
    public String asSummaryString() {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("catalogModel", catalogModel.toProperties());
        params.put("identifier", modelIdentifier);
        params.put("ignoreIfExists", ignoreIfExists);
        params.put("isTemporary", isTemporary);

        return OperationUtils.formatWithChildren(
                "CREATE MODEL", params, Collections.emptyList(), Operation::asSummaryString);
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        if (isTemporary) {
            ctx.getCatalogManager()
                    .createTemporaryModel(catalogModel, modelIdentifier, ignoreIfExists);
        } else {
            ctx.getCatalogManager().createModel(catalogModel, modelIdentifier, ignoreIfExists);
        }
        return TableResultImpl.TABLE_RESULT_OK;
    }
}
