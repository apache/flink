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
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.exceptions.ModelAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.ModelNotExistException;

/** Operation to describe a ALTER MODEL .. RENAME TO .. statement. */
@Internal
public class AlterModelRenameOperation implements AlterOperation {

    private final ObjectIdentifier modelIdentifier;
    private final ObjectIdentifier newModelIdentifier;
    private final boolean ignoreIfNotExists;

    /**
     * Creates a AlterModelRenameOperation.
     *
     * @param modelIdentifier The identifier of the model to rename.
     * @param newModelIdentifier The new identifier of the model.
     * @param ignoreIfNotExists A flag that indicates if the operation should throw an exception if
     *     model does not exist.
     */
    public AlterModelRenameOperation(
            ObjectIdentifier modelIdentifier,
            ObjectIdentifier newModelIdentifier,
            boolean ignoreIfNotExists) {
        this.modelIdentifier = modelIdentifier;
        this.newModelIdentifier = newModelIdentifier;
        this.ignoreIfNotExists = ignoreIfNotExists;
    }

    public ObjectIdentifier getModelIdentifier() {
        return modelIdentifier;
    }

    public ObjectIdentifier getNewModelIdentifier() {
        return newModelIdentifier;
    }

    public boolean ignoreIfNotExists() {
        return ignoreIfNotExists;
    }

    @Override
    public String asSummaryString() {
        return String.format(
                "ALTER MODEL %s%s RENAME TO %s",
                ignoreIfNotExists ? "IF EXISTS " : "",
                modelIdentifier.asSummaryString(),
                newModelIdentifier.asSummaryString());
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        try {
            final Catalog catalog =
                    ctx.getCatalogManager()
                            .getCatalogOrThrowException(getModelIdentifier().getCatalogName());

            catalog.renameModel(
                    getModelIdentifier().toObjectPath(),
                    getNewModelIdentifier().getObjectName(),
                    ignoreIfNotExists());
            return TableResultImpl.TABLE_RESULT_OK;
        } catch (ModelAlreadyExistException | ModelNotExistException e) {
            throw new ValidationException(
                    String.format("Could not execute %s", asSummaryString()), e);
        }
    }
}
