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

package org.apache.flink.table.gateway.service.result;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.CollectionUtil;

import java.util.Arrays;
import java.util.List;
import java.util.Spliterators;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.flink.table.gateway.api.config.SqlGatewayServiceConfigOptions.SQL_GATEWAY_SECURITY_REDACT_SENSITIVE_OPTIONS_ENABLED;
import static org.apache.flink.table.gateway.api.config.SqlGatewayServiceConfigOptions.SQL_GATEWAY_SECURITY_REDACT_SENSITIVE_OPTIONS_NAMES;

public class ShowCreateResultCollector {

    private final boolean enabled;
    private final Pattern sensitiveOptions;

    public ShowCreateResultCollector(Configuration configuration) {
        this(
                configuration.get(SQL_GATEWAY_SECURITY_REDACT_SENSITIVE_OPTIONS_ENABLED),
                configuration.get(SQL_GATEWAY_SECURITY_REDACT_SENSITIVE_OPTIONS_NAMES).split(","));
    }

    ShowCreateResultCollector(Boolean enabled, String[] sensitiveOptions) {
        this.enabled = enabled;
        this.sensitiveOptions = createPattern(sensitiveOptions);
    }

    private static Pattern createPattern(String[] sensitiveOptions) {
        String[] optionsQuoted =
                Arrays.stream(sensitiveOptions).map(Pattern::quote).toArray(String[]::new);
        var pattern = "[^']*" + String.join("[^']*|[^']*", optionsQuoted) + "[^']*";
        return Pattern.compile("'(" + pattern + ")'\\s*=\\s*'[^']*'", Pattern.CASE_INSENSITIVE);
    }

    public List<RowData> collect(TableResultInternal tableResult) {
        if (!enabled) {
            return CollectionUtil.iteratorToList(tableResult.collectInternal());
        }
        if (tableResult.getResolvedSchema().getColumnCount() != 1) {
            throw new IllegalArgumentException(
                    "Sensitive options protection not supported for multiple columns");
        }
        if (!(tableResult.getResolvedSchema().getColumnDataTypes().get(0).getLogicalType()
                instanceof VarCharType)) {
            throw new IllegalArgumentException("Operation supported only for text column");
        }
        var spliterator = Spliterators.spliteratorUnknownSize(tableResult.collectInternal(), 0);
        return StreamSupport.stream(spliterator, false)
                .map(this::maskSensitive)
                .collect(Collectors.toList());
    }

    private RowData maskSensitive(RowData rowData) {
        var stringData = StringData.fromString(maskSensitive(rowData.getString(0).toString()));
        return GenericRowData.ofKind(rowData.getRowKind(), stringData);
    }

    private String maskSensitive(String ddl) {
        Matcher matcher = sensitiveOptions.matcher(ddl);
        return matcher.replaceAll("'$1' = '****'");
    }
}
