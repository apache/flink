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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.ShowCreateUtil;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;

import static org.apache.flink.table.api.internal.TableResultUtils.buildStringArrayResult;

/** Operation to describe a SHOW CREATE VIEW statement. */
@Internal
public class ShowCreateViewOperation implements ShowOperation {

    private final ObjectIdentifier viewIdentifier;

    public ShowCreateViewOperation(ObjectIdentifier sqlIdentifier) {
        this.viewIdentifier = sqlIdentifier;
    }

    public ObjectIdentifier getViewIdentifier() {
        return viewIdentifier;
    }

    @Override
    public String asSummaryString() {
        return String.format("SHOW CREATE VIEW %s", viewIdentifier.asSummaryString());
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        final ContextResolvedTable table =
                ctx.getCatalogManager()
                        .getTable(viewIdentifier)
                        .orElseThrow(
                                () ->
                                        new ValidationException(
                                                String.format(
                                                        "Could not execute SHOW CREATE VIEW. View with identifier %s does not exist.",
                                                        viewIdentifier.asSerializableString())));

        String resultRow =
                ShowCreateUtil.buildShowCreateViewRow(
                        table.getResolvedTable(), viewIdentifier, table.isTemporary());

        return buildStringArrayResult("result", new String[] {resultRow});
    }
}
