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
import org.apache.flink.table.catalog.ModelChange;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.operations.OperationUtils;

import javax.annotation.Nullable;

import java.util.List;
import java.util.stream.Collectors;

/** Operation to describe a ALTER MODEL .. SET .. statement. */
@Internal
public class AlterModelChangeOperation implements AlterOperation {

    private final ObjectIdentifier modelIdentifier;
    private final List<ModelChange> modelChanges;
    private final CatalogModel catalogModel;
    private final boolean ignoreIfNotExists;

    public ObjectIdentifier getModelIdentifier() {
        return modelIdentifier;
    }

    public List<ModelChange> getModelChanges() {
        return modelChanges;
    }

    @Nullable
    public CatalogModel getCatalogModel() {
        return catalogModel;
    }

    public boolean ignoreIfNotExists() {
        return ignoreIfNotExists;
    }

    /**
     * Creates an ALTER MODEL CHANGE statement.
     *
     * @param modelIdentifier The identifier of the model to be altered.
     * @param modelChanges The list of changes to be applied to the model.
     * @param catalogModel The resolved model after applying the changes. If null, existing model
     *     doesn't exist and ignoreIfNotExists is true.
     * @param ignoreIfNotExists Flag to specify behavior when the model doesn't exist.
     */
    public AlterModelChangeOperation(
            ObjectIdentifier modelIdentifier,
            List<ModelChange> modelChanges,
            @Nullable CatalogModel catalogModel,
            boolean ignoreIfNotExists) {
        this.modelIdentifier = modelIdentifier;
        this.modelChanges = modelChanges;
        this.catalogModel = catalogModel;
        this.ignoreIfNotExists = ignoreIfNotExists;
    }

    @Override
    public String asSummaryString() {
        String changes =
                modelChanges.stream()
                        .map(AlterModelChangeOperation::toString)
                        .collect(Collectors.joining(",\n"));
        return String.format(
                "ALTER MODEL %s%s\n%s",
                ignoreIfNotExists ? "IF EXISTS " : "", modelIdentifier.asSummaryString(), changes);
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        if (getCatalogModel() == null && ignoreIfNotExists()) {
            return TableResultImpl.TABLE_RESULT_OK;
        }

        ctx.getCatalogManager()
                .alterModel(
                        getCatalogModel(), modelChanges, getModelIdentifier(), ignoreIfNotExists());
        return TableResultImpl.TABLE_RESULT_OK;
    }

    private static String toString(ModelChange modelChange) {
        if (modelChange instanceof ModelChange.SetOption) {
            ModelChange.SetOption setOption = (ModelChange.SetOption) modelChange;
            return String.format(
                    "  SET (%s)",
                    OperationUtils.formatParameter(setOption.getKey(), setOption.getValue()));
        } else if (modelChange instanceof ModelChange.ResetOption) {
            ModelChange.ResetOption resetOption = (ModelChange.ResetOption) modelChange;
            return String.format("  RESET (%s)", resetOption.getKey());
        } else {
            throw new UnsupportedOperationException(
                    String.format("Unknown model change: %s", modelChange));
        }
    }
}
