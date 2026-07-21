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
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.CatalogConnection;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.types.DataType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.flink.table.api.internal.TableResultUtils.buildTableResult;

/** Operation to describe a DESCRIBE CONNECTION statement. */
@Internal
public class DescribeConnectionOperation implements Operation, ExecutableOperation {

    private static final String CONNECTION_SECRET_REFERENCE_KEY = "__flink.encrypted-secret-key__";

    private final ObjectIdentifier connectionIdentifier;
    private final boolean isExtended;

    public DescribeConnectionOperation(ObjectIdentifier connectionIdentifier, boolean isExtended) {
        this.connectionIdentifier = connectionIdentifier;
        this.isExtended = isExtended;
    }

    public ObjectIdentifier getConnectionIdentifier() {
        return connectionIdentifier;
    }

    public boolean isExtended() {
        return isExtended;
    }

    @Override
    public String asSummaryString() {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("identifier", connectionIdentifier);
        params.put("isExtended", isExtended);
        return OperationUtils.formatWithChildren(
                "DESCRIBE CONNECTION", params, Collections.emptyList(), Operation::asSummaryString);
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        CatalogConnection connection =
                ctx.getCatalogManager()
                        .getConnection(connectionIdentifier)
                        .orElseThrow(
                                () ->
                                        new ValidationException(
                                                String.format(
                                                        "Connection with identifier '%s' does not exist.",
                                                        connectionIdentifier.asSummaryString())));
        boolean isTemporary = ctx.getCatalogManager().isTemporaryConnection(connectionIdentifier);

        return buildTableResult(
                new String[] {"name", "value"},
                new DataType[] {DataTypes.STRING(), DataTypes.STRING()},
                buildRows(
                        connection,
                        isTemporary,
                        ctx.getTableConfig().get(SecurityOptions.ADDITIONAL_SENSITIVE_KEYS)));
    }

    private Object[][] buildRows(
            CatalogConnection connection,
            boolean isTemporary,
            List<String> additionalSensitiveKeys) {
        List<Object[]> rows = new ArrayList<>();
        new TreeMap<>(connection.getOptions())
                .forEach(
                        (key, value) -> {
                            if (!CONNECTION_SECRET_REFERENCE_KEY.equals(key)) {
                                rows.add(
                                        new Object[] {
                                            key, maskValue(key, value, additionalSensitiveKeys)
                                        });
                            }
                        });
        if (connection.getComment() != null && !connection.getComment().isEmpty()) {
            rows.add(new Object[] {"comment", connection.getComment()});
        }
        if (isExtended) {
            rows.add(new Object[] {"temporary", String.valueOf(isTemporary)});
        }
        return rows.toArray(new Object[0][]);
    }

    private String maskValue(String key, String value, List<String> additionalSensitiveKeys) {
        return GlobalConfiguration.isSensitive(key, additionalSensitiveKeys)
                ? GlobalConfiguration.HIDDEN_CONTENT
                : value;
    }
}
