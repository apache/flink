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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.CatalogDescriptor;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.types.DataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.api.internal.TableResultUtils.buildTableResult;

/** Operation to describe a DESCRIBE CATALOG catalog_name statement. */
@Internal
public class DescribeCatalogOperation implements Operation, ExecutableOperation {

    private final String catalogName;
    private final boolean isExtended;

    public DescribeCatalogOperation(String catalogName, boolean isExtended) {
        this.catalogName = catalogName;
        this.isExtended = isExtended;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public boolean isExtended() {
        return isExtended;
    }

    @Override
    public String asSummaryString() {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("identifier", catalogName);
        params.put("isExtended", isExtended);
        return OperationUtils.formatWithChildren(
                "DESCRIBE CATALOG", params, Collections.emptyList(), Operation::asSummaryString);
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        CatalogDescriptor catalogDescriptor =
                ctx.getCatalogManager()
                        .getCatalogDescriptor(catalogName)
                        .orElseThrow(
                                () ->
                                        new ValidationException(
                                                String.format(
                                                        "Cannot obtain metadata information from Catalog %s.",
                                                        catalogName)));
        Map<String, String> properties = catalogDescriptor.getConfiguration().toMap();
        List<List<Object>> rows =
                new ArrayList<>(
                        Arrays.asList(
                                Arrays.asList("name", catalogName),
                                Arrays.asList(
                                        "type",
                                        properties.getOrDefault(
                                                CommonCatalogOptions.CATALOG_TYPE.key(), "")),
                                Arrays.asList(
                                        "comment", catalogDescriptor.getComment().orElse(null))));
        if (isExtended) {
            properties.entrySet().stream()
                    .filter(
                            entry ->
                                    !CommonCatalogOptions.CATALOG_TYPE.key().equals(entry.getKey()))
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(
                            entry ->
                                    rows.add(
                                            Arrays.asList(
                                                    String.format("option:%s", entry.getKey()),
                                                    entry.getValue())));
        }

        return buildTableResult(
                new String[] {"info name", "info value"},
                new DataType[] {DataTypes.STRING(), DataTypes.STRING()},
                rows.stream().map(List::toArray).toArray(Object[][]::new));
    }
}
