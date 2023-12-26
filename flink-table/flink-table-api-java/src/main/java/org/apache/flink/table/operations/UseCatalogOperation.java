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
import org.apache.flink.table.api.internal.TableResultImpl;
import org.apache.flink.table.api.internal.TableResultInternal;

/** Operation to describe a USE CATALOG statement. */
@Internal
public class UseCatalogOperation implements UseOperation {

    private final String catalogName;

    public UseCatalogOperation(String catalogName) {
        this.catalogName = catalogName;
    }

    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public String asSummaryString() {
        return String.format("USE CATALOG %s", catalogName);
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        ctx.getCatalogManager().setCurrentCatalog(catalogName);
        return TableResultImpl.TABLE_RESULT_OK;
    }
}
