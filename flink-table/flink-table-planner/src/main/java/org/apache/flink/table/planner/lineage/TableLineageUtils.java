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
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.operations.ModifyType;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Util class for building table lineage graph. */
public class TableLineageUtils {

    /** Extract optional lineage info from output format, sink, or sink function. */
    public static Optional<LineageVertex> extractLineageDataset(@Nullable Object object) {
        if (object != null && object instanceof LineageVertexProvider) {
            return Optional.of(((LineageVertexProvider) object).getLineageVertex());
        }

        return Optional.empty();
    }

    public static LineageDataset createTableLineageDataset(
            ContextResolvedTable contextResolvedTable, Optional<LineageVertex> lineageDataset) {
        String name = contextResolvedTable.getIdentifier().asSummaryString();
        TableLineageDatasetImpl tableLineageDataset =
                new TableLineageDatasetImpl(
                        contextResolvedTable, findLineageDataset(name, lineageDataset));

        return tableLineageDataset;
    }

    public static ModifyType convert(ChangelogMode inputChangelogMode) {
        if (inputChangelogMode.containsOnly(RowKind.INSERT)) {
            return ModifyType.INSERT;
        } else if (inputChangelogMode.containsOnly(RowKind.DELETE)) {
            return ModifyType.DELETE;
        } else {
            return ModifyType.UPDATE;
        }
    }

    private static Optional<LineageDataset> findLineageDataset(
            String name, Optional<LineageVertex> lineageVertexOpt) {
        if (lineageVertexOpt.isPresent()) {
            LineageVertex lineageVertex = lineageVertexOpt.get();
            if (lineageVertex.datasets().size() == 1) {
                return Optional.of(lineageVertex.datasets().get(0));
            }

            for (LineageDataset dataset : lineageVertex.datasets()) {
                if (dataset.name().equals(name)) {
                    return Optional.of(dataset);
                }
            }
        }

        return Optional.empty();
    }

    private static Map<String, String> extractOptions(CatalogBaseTable catalogBaseTable) {
        try {
            return catalogBaseTable.getOptions();
        } catch (Exception e) {
            return new HashMap<>();
        }
    }
}
