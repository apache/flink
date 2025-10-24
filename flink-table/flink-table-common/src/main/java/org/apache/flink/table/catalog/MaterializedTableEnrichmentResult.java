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

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshMode;

/**
 * Result of the enrichment process containing the resolved freshness interval and physical refresh
 * mode for a {@link CatalogMaterializedTable}.
 *
 * <p>This object is returned by {@link MaterializedTableEnricher} after determining the final,
 * non-null values for both properties.
 */
@Experimental
public class MaterializedTableEnrichmentResult {

    private final IntervalFreshness freshness;
    private final RefreshMode refreshMode;

    public MaterializedTableEnrichmentResult(
            final IntervalFreshness freshness, final RefreshMode refreshMode) {
        this.freshness = freshness;
        this.refreshMode = refreshMode;
    }

    public IntervalFreshness getFreshness() {
        return freshness;
    }

    public RefreshMode getRefreshMode() {
        return refreshMode;
    }
}
