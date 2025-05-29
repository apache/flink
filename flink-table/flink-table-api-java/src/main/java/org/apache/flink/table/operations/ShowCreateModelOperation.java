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
import org.apache.flink.table.api.internal.ShowCreateUtil;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogModel;

import static org.apache.flink.table.api.internal.TableResultUtils.buildStringArrayResult;

/** Operation to describe a SHOW CREATE MODEL statement. */
@Internal
public class ShowCreateModelOperation implements ShowOperation {

    private final ObjectIdentifier modelIdentifier;
    private final ResolvedCatalogModel model;
    private final boolean isTemporary;

    public ShowCreateModelOperation(
            ObjectIdentifier sqlIdentifier, ResolvedCatalogModel model, boolean isTemporary) {
        this.modelIdentifier = sqlIdentifier;
        this.model = model;
        this.isTemporary = isTemporary;
    }

    public ResolvedCatalogModel getModel() {
        return model;
    }

    @Override
    public String asSummaryString() {
        return String.format("SHOW CREATE MODEL %s", modelIdentifier.asSummaryString());
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        String resultRow =
                ShowCreateUtil.buildShowCreateModelRow(model, modelIdentifier, isTemporary);
        return buildStringArrayResult("result", new String[] {resultRow});
    }
}
