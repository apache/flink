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
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.ObjectIdentifier;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Operation to describe a DESCRIBE [EXTENDED] [[catalogName.] dataBasesName].sqlIdentifier
 * statement.
 */
@Internal
public class DescribeTableOperation implements Operation, ExecutableOperation {

    private final ObjectIdentifier sqlIdentifier;
    private final boolean isExtended;

    public DescribeTableOperation(ObjectIdentifier sqlIdentifier, boolean isExtended) {
        this.sqlIdentifier = sqlIdentifier;
        this.isExtended = isExtended;
    }

    public ObjectIdentifier getSqlIdentifier() {
        return sqlIdentifier;
    }

    public boolean isExtended() {
        return isExtended;
    }

    @Override
    public String asSummaryString() {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("identifier", sqlIdentifier);
        params.put("isExtended", isExtended);
        return OperationUtils.formatWithChildren(
                "DESCRIBE", params, Collections.emptyList(), Operation::asSummaryString);
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        // DESCRIBE <table> is a synonym for SHOW COLUMNS without LIKE pattern.
        ShowColumnsOperation showColumns =
                new ShowColumnsOperation(sqlIdentifier, null, false, false, "FROM");
        return showColumns.execute(ctx);
    }
}
