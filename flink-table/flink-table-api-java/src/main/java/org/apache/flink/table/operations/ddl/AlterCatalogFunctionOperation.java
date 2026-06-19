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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.TableResultImpl;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.OperationUtils;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Operation to describe a ALTER FUNCTION statement for catalog functions. */
@Internal
public class AlterCatalogFunctionOperation implements AlterOperation {
    private final ObjectIdentifier functionIdentifier;
    private final CatalogFunction catalogFunction;
    private final boolean ifExists;
    private final boolean isTemporary;

    public AlterCatalogFunctionOperation(
            ObjectIdentifier functionIdentifier,
            CatalogFunction catalogFunction,
            boolean ifExists,
            boolean isTemporary) {
        this.functionIdentifier = functionIdentifier;
        this.catalogFunction = catalogFunction;
        this.ifExists = ifExists;
        this.isTemporary = isTemporary;
    }

    public CatalogFunction getCatalogFunction() {
        return this.catalogFunction;
    }

    public ObjectIdentifier getFunctionIdentifier() {
        return this.functionIdentifier;
    }

    public boolean isIfExists() {
        return this.ifExists;
    }

    public boolean isTemporary() {
        return isTemporary;
    }

    @Override
    public String asSummaryString() {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("catalogFunction", catalogFunction.getDetailedDescription());
        params.put("identifier", functionIdentifier);
        params.put("ifExists", ifExists);
        params.put("isTemporary", isTemporary);

        return OperationUtils.formatWithChildren(
                "ALTER CATALOG FUNCTION",
                params,
                Collections.emptyList(),
                Operation::asSummaryString);
    }

    public String getFunctionName() {
        return this.functionIdentifier.getObjectName();
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        try {
            CatalogFunction function = getCatalogFunction();
            if (isTemporary()) {
                throw new ValidationException("Alter temporary catalog function is not supported");
            } else {
                Catalog catalog =
                        ctx.getCatalogManager()
                                .getCatalogOrThrowException(
                                        getFunctionIdentifier().getCatalogName());
                catalog.alterFunction(
                        getFunctionIdentifier().toObjectPath(), function, isIfExists());
            }
            return TableResultImpl.TABLE_RESULT_OK;
        } catch (ValidationException e) {
            throw e;
        } catch (FunctionNotExistException e) {
            throw new ValidationException(e.getMessage(), e);
        } catch (Exception e) {
            throw new TableException(String.format("Could not execute %s", asSummaryString()), e);
        }
    }
}
