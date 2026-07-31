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
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.SensitiveConnection;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.OperationUtils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Operation to describe a CREATE CONNECTION statement. */
@Internal
public class CreateConnectionOperation implements CreateOperation {

    private static final String MASKED_VALUE = "****";

    private final ObjectIdentifier connectionIdentifier;
    private final SensitiveConnection sensitiveConnection;
    private final boolean ignoreIfExists;
    private final boolean isTemporary;

    public CreateConnectionOperation(
            ObjectIdentifier connectionIdentifier,
            SensitiveConnection sensitiveConnection,
            boolean ignoreIfExists,
            boolean isTemporary) {
        this.connectionIdentifier = connectionIdentifier;
        this.sensitiveConnection = sensitiveConnection;
        this.ignoreIfExists = ignoreIfExists;
        this.isTemporary = isTemporary;
    }

    public ObjectIdentifier getConnectionIdentifier() {
        return connectionIdentifier;
    }

    public SensitiveConnection getSensitiveConnection() {
        return sensitiveConnection;
    }

    public boolean isIgnoreIfExists() {
        return ignoreIfExists;
    }

    public boolean isTemporary() {
        return isTemporary;
    }

    @Override
    public String asSummaryString() {
        Map<String, String> maskedOptions =
                sensitiveConnection.getOptions().entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        e -> MASKED_VALUE,
                                        (a, b) -> a,
                                        LinkedHashMap::new));
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("connectionOptions", maskedOptions);
        params.put("identifier", connectionIdentifier);
        params.put("ignoreIfExists", ignoreIfExists);
        params.put("isTemporary", isTemporary);

        return OperationUtils.formatWithChildren(
                "CREATE CONNECTION", params, List.of(), Operation::asSummaryString);
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        if (isTemporary) {
            ctx.getCatalogManager()
                    .createTemporaryConnection(
                            sensitiveConnection, connectionIdentifier, ignoreIfExists);
        } else {
            ctx.getCatalogManager()
                    .createConnection(sensitiveConnection, connectionIdentifier, ignoreIfExists);
        }
        return TableResultImpl.TABLE_RESULT_OK;
    }
}
