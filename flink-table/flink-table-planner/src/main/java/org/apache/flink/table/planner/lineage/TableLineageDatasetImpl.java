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

package org.apache.flink.table.planner.lineage;

import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageDatasetFacet;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.listener.CatalogContext;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Implementation for TableLineageDataSet. */
public class TableLineageDatasetImpl implements TableLineageDataset {
    @JsonProperty private String name;
    @JsonProperty private String namespace;
    private CatalogContext catalogContext;
    private CatalogBaseTable catalogBaseTable;
    @JsonProperty private ObjectPath objectPath;
    @JsonProperty private Map<String, LineageDatasetFacet> facets;

    public TableLineageDatasetImpl(
            ContextResolvedTable contextResolvedTable, Optional<LineageDataset> lineageDatasetOpt) {
        this.name = contextResolvedTable.getIdentifier().asSummaryString();
        this.namespace =
                lineageDatasetOpt.map(lineageDataset -> lineageDataset.namespace()).orElse("");
        this.catalogContext =
                CatalogContext.createContext(
                        contextResolvedTable.getCatalog().isPresent()
                                ? ((AbstractCatalog) contextResolvedTable.getCatalog().get())
                                        .getName()
                                : "",
                        contextResolvedTable.getCatalog().orElse(null));
        this.catalogBaseTable = contextResolvedTable.getTable();
        this.objectPath =
                contextResolvedTable.isAnonymous()
                        ? null
                        : contextResolvedTable.getIdentifier().toObjectPath();
        this.facets = new HashMap<>();
        if (lineageDatasetOpt.isPresent()) {
            this.facets.putAll(lineageDatasetOpt.get().facets());
        }
    }

    public void addLineageDatasetFacet(LineageDatasetFacet facet) {
        facets.put(facet.name(), facet);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String namespace() {
        return namespace;
    }

    @Override
    public Map<String, LineageDatasetFacet> facets() {
        return facets;
    }

    @Override
    public CatalogContext catalogContext() {
        return catalogContext;
    }

    @Override
    public CatalogBaseTable table() {
        return catalogBaseTable;
    }

    @Override
    public ObjectPath objectPath() {
        return objectPath;
    }
}
