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
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.table.api.internal.ShowCreateUtil;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.CatalogConnection;
import org.apache.flink.table.catalog.ObjectIdentifier;

import static org.apache.flink.table.api.internal.TableResultUtils.buildStringArrayResult;

/** Operation to describe a SHOW CREATE CONNECTION statement. */
@Internal
public class ShowCreateConnectionOperation implements ShowOperation {

    private final ObjectIdentifier connectionIdentifier;
    private final CatalogConnection connection;
    private final boolean isTemporary;

    public ShowCreateConnectionOperation(
            ObjectIdentifier connectionIdentifier,
            CatalogConnection connection,
            boolean isTemporary) {
        this.connectionIdentifier = connectionIdentifier;
        this.connection = connection;
        this.isTemporary = isTemporary;
    }

    public ObjectIdentifier getConnectionIdentifier() {
        return connectionIdentifier;
    }

    public CatalogConnection getConnection() {
        return connection;
    }

    public boolean isTemporary() {
        return isTemporary;
    }

    @Override
    public String asSummaryString() {
        return String.format("SHOW CREATE CONNECTION %s", connectionIdentifier.asSummaryString());
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        String resultRow =
                ShowCreateUtil.buildShowCreateConnectionRow(
                        connection,
                        connectionIdentifier,
                        isTemporary,
                        ctx.getTableConfig().get(SecurityOptions.ADDITIONAL_SENSITIVE_KEYS));
        return buildStringArrayResult("result", new String[] {resultRow});
    }
}
