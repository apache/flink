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

package org.apache.flink.table.endpoint.hive.util;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.gateway.api.SqlGatewayService;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.gateway.api.session.SessionHandle;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import static org.apache.flink.table.endpoint.hive.HiveServer2Schemas.GET_CATALOGS_SCHEMA;
import static org.apache.flink.table.endpoint.hive.HiveServer2Schemas.GET_SCHEMAS_SCHEMA;
import static org.apache.flink.table.gateway.api.results.ResultSet.ResultType.EOS;

/** Factory to create the operation executor. */
public class OperationExecutorFactory {

    public static Callable<ResultSet> createGetCatalogsExecutor(
            SqlGatewayService service, SessionHandle sessionHandle) {
        return () -> executeGetCatalogs(service, sessionHandle);
    }

    public static Callable<ResultSet> createGetSchemasExecutor(
            SqlGatewayService service,
            SessionHandle sessionHandle,
            String catalogName,
            String schemaName) {
        return () -> executeGetSchemas(service, sessionHandle, catalogName, schemaName);
    }

    private static ResultSet executeGetCatalogs(
            SqlGatewayService service, SessionHandle sessionHandle) {
        Set<String> catalogNames = service.listCatalogs(sessionHandle);
        return new ResultSet(
                EOS,
                null,
                GET_CATALOGS_SCHEMA,
                catalogNames.stream()
                        .map(OperationExecutorFactory::packData)
                        .collect(Collectors.toList()));
    }

    private static ResultSet executeGetSchemas(
            SqlGatewayService service,
            SessionHandle sessionHandle,
            String catalogName,
            String schemaName) {
        Set<String> schemaNames =
                filterStringSetBy(service.listDatabases(sessionHandle, catalogName), schemaName);

        return new ResultSet(
                EOS,
                null,
                GET_SCHEMAS_SCHEMA,
                schemaNames.stream()
                        .map(name -> packData(name, catalogName))
                        .collect(Collectors.toList()));
    }

    // -------------------------------------
    // useful methods
    // -------------------------------------

    /**
     * Covert SQL 'like' pattern to a Java regular expression. Underscores (_) are converted to '.'
     * and percent signs (%) are converted to '.*'. Note: escape characters are removed.
     *
     * @param pattern the SQL pattern to convert.
     * @return the equivalent Java regular expression of the pattern.
     */
    private static String convertNamePattern(String pattern) {
        if ((pattern == null) || pattern.isEmpty()) {
            pattern = "%";
        }
        String wStr = ".*";
        return pattern.replaceAll("([^\\\\])%", "$1" + wStr)
                .replaceAll("\\\\%", "%")
                .replaceAll("^%", wStr)
                .replaceAll("([^\\\\])_", "$1.")
                .replaceAll("\\\\_", "_")
                .replaceAll("^_", ".");
    }

    public static Set<String> filterStringSetBy(Set<String> set, String pattern) {
        return set.stream()
                .filter(name -> name.matches(convertNamePattern(pattern)))
                .collect(Collectors.toSet());
    }

    private static GenericRowData packData(List<Object> data) {
        Object[] pack = new Object[data.size()];
        for (int i = 0; i < data.size(); i++) {
            if (data.get(i) != null) {
                if (data.get(i) instanceof String) {
                    pack[i] = StringData.fromString((String) data.get(i));
                } else if (data.get(i) instanceof Integer) {
                    pack[i] = data.get(i);
                } else if (data.get(i) instanceof Short) {
                    pack[i] = data.get(i);
                } else {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Data type '%s' of '%s' not supported.",
                                    data.get(i).getClass(), data.get(i)));
                }
            }
        }
        return GenericRowData.of(pack);
    }

    private static GenericRowData packData(Object... data) {
        return packData(Arrays.stream(data).collect(Collectors.toList()));
    }
}
